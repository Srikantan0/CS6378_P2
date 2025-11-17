# PowerShell version of cleanup.sh

# --- Configuration ---
$netid = "dal660753"

# Root directory of your project on remote servers
$PROJDIR = "/home/012/g/gg/dal660753/TestProj"

# Local path to config file
$CONFIGLOCAL = "C:\Users\Srikantan\Downloads\config.txt"

# Remote output directory
$OUTPUTDIR = "/tmp/dal660753/output"

# JAR file (unused here, but defined for consistency)
$JAR_FILE = "./target/node-1.0-SNAPSHOT.jar"

# --- Step 1: Read and preprocess configuration file ---
$CONFIG = Get-Content $CONFIGLOCAL | ForEach-Object {
    $_ -replace '#.*',''     # Remove comments
} | Where-Object { $_ -match '\S' }  # Skip blank lines

# --- Step 2: Parse global parameters and node info ---
$i = 0
$n = 0
$NODES = @()

foreach ($line in $CONFIG) {
    if ($i -eq 0) {
        # Global parameters: n minPerActive maxPerActive minSendDelay snapshotDelay maxNumber
        $tokens = $line -split '\s+'
        $n = [int]$tokens[0]
        $minPerActive = $tokens[1]
        $maxPerActive = $tokens[2]
        $minSendDelay = $tokens[3]
        $snapshotDelay = $tokens[4]
        $maxNumber = $tokens[5]
        Write-Host "[DEBUG] Global Params: {$n, $minPerActive, $maxPerActive, $minSendDelay, $snapshotDelay, $maxNumber}"
    }
    elseif ($i -le $n) {
        $NODES += $line
    }
    $i++
}

# --- Step 3: Cleanup remote nodes ---
for ($i = 0; $i -lt $n; $i++) {
    $parts = $NODES[$i] -split '\s+'
    $id = $parts[0]
    $hostname = $parts[1]
    $port = $parts[2]

    Write-Host "[DEBUG] Cleanup on Node $i config: id=$id host=$hostname port=$port"

    $remoteCmd = "rm -rf $OUTPUTDIR/ && killall -u $netid"
    Write-Host "[DEBUG] ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $netid@$hostname $remoteCmd"

    ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no "$($netid)@$($hostname)" $remoteCmd
}

Write-Host "Cleanup complete."
