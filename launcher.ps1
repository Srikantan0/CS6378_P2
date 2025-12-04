# --- Configuration ---
$netid = "dal660753"

# Root directory of your project on remote servers
$PROJDIR = "/home/012/g/gg/dal660753/TestProj"

# Local path to config file
$CONFIGLOCAL = "C:\Users\Srikantan\Downloads\config.txt"

# Remote output directory
$OUTPUTDIR = "/tmp/dal660753/output"

# JAR file (wildcard supported)
$JAR_FILE = ".\target\Node-1.0-SNAPSHOT.jar"

# --- Step 1: Read and preprocess configuration file ---
$CONFIG = Get-Content $CONFIGLOCAL | ForEach-Object {
    $_ -replace '#.*',''
} | Where-Object { $_ -match '\S' }

# --- Step 2: Parse global parameters and node info ---
$i = 0
$n = 0
$NODES = @()

foreach ($line in $CONFIG) {
    if ($i -eq 0) {
        # Global parameters: n interRequestDelay csExecutionTime numOfRequests
        $tokens = $line -split '\s+'
        $n = [int]$tokens[0]
        $interRequestDelay = $tokens[1]
        $csExecutionTime = $tokens[2]
        $numOfRequests = $tokens[3]
        Write-Host "[DEBUG] Global Params: {$n, $interRequestDelay, $csExecutionTime, $numOfRequests}"
    }
    elseif ($i -le $n) {
        $NODES += $line
    }
    $i++
}

# --- Step 3: Deploy and run all nodes ---
for ($i = 0; $i -lt $n; $i++) {
    $parts = $NODES[$i] -split '\s+'
    $id = $parts[0]
    $hostname = $parts[1]
    $port = $parts[2]

    Write-Host "[DEBUG] Deploying to node $i config: id=$id host=$hostname port=$port"

    # 1. Create directories remotely
    $cmd_mkdir = "mkdir -p $PROJDIR $OUTPUTDIR"
    Write-Host "[DEBUG] ssh $netid@$hostname $cmd_mkdir"
    ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no "$($netid)@$($hostname)" $cmd_mkdir

    # 2. Copy files (JAR + config)
    $remotePath = "$($netid)@$($hostname):$PROJDIR"
    Write-Host "[DEBUG] scp $JAR_FILE $CONFIGLOCAL $remotePath"
    scp $JAR_FILE $CONFIGLOCAL $remotePath

    # 3. Launch Java node in background
    $remoteCmd = "java -jar $PROJDIR/Node-1.0-SNAPSHOT.jar $id $PROJDIR/config.txt $OUTPUTDIR > $OUTPUTDIR/node-$id.log 2>&1 &"
    Write-Host "[DEBUG] ssh $netid@$hostname $remoteCmd"
    ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no "$($netid)@$($hostname)" $remoteCmd

    Start-Sleep -Seconds 1
}

Write-Host "[INFO] Launched application in $n nodes"
