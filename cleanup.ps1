# --- Configuration ---
$netid = "dal660753"

# Root directory of your project on remote servers
$PROJDIR = "/home/012/g/gg/dal660753/TestProj"

# Local path to config file
$CONFIGLOCAL = "C:\Users\Srikantan\Downloads\config.txt"

# Remote output directory
$OUTPUTDIR = "/tmp/dal660753/output"

# JAR file (wildcard supported)
$JAR_FILE = ".\target\node-1.0-SNAPSHOT.jar"

# --- Step 1: Pre-process configuration and store in a variable ---
# Read config, remove comments and blank lines
$CONFIG = Get-Content $CONFIGLOCAL | ForEach-Object { $_ -replace "#.*", "" } | Where-Object { $_.Trim() -ne "" }

# --- Step 2: Read and Separate Global/Node Parameters ---
$i = 0
$n = 0
$NODES = @()

foreach ($line in $CONFIG) {
    if ($i -eq 0) {
        # Line 0: Global Parameters
        $params = $line -split '\s+'
        $n = [int]$params[0]
        $d = $params[1]
        $c = $params[2]
        $m = $params[3]
        Write-Host "[DEBUG] Global Params: {$n, $d, $c, $m}"
    }
    elseif ($i -le $n) {
        # Lines 1 to n: Node Configuration (ID, Host, Port)
        $NODES += $line
    }
    # Ignore neighbor lines and any remaining lines for this cleanup stage
    $i++
}

# --- Step 3: Cleanup on all nodes ---
for ($i = 0; $i -lt $n; $i++) {
    # Read node details from the array
    $nodeParams = $NODES[$i] -split '\s+'
    $id = $nodeParams[0]
    $hostName = $nodeParams[1]
    $port = $nodeParams[2]

    $sshTarget = "$netid@$hostName"

    Write-Host "[DEBUG] Cleanup on Node $i config: id=$id host=$hostName port=$port"
    Write-Host "[DEBUG] ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no `"$sshTarget`" `"rm -rf $OUTPUTDIR/ && killall -u $netid`""
    ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $sshTarget "rm -rf $OUTPUTDIR/ node*.log && killall -u $netid"
}

Write-Host "Cleanup complete"