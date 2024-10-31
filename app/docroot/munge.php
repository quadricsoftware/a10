<?php
include_once(__DIR__."/../libs/main_lib");
//$headers = getallheaders();

//logme("Incoming: ". $_SERVER['REQUEST_URI']);

//$sapi = php_sapi_name();


handleAuth($_SERVER['HTTP_AUTHORIZATION']);

//logme("Passed auth: ". $_SERVER['HTTP_AUTHORIZATION']);

// this splits without dropping the char '0'
$bits = preg_split('/\//', $_SERVER['REQUEST_URI'], -1, PREG_SPLIT_NO_EMPTY);
array_shift($bits);	// drop the "/data/" part

//$all = print_r($bits,true);
//logme("Got request, all bits: $all");

if($_SERVER['REQUEST_METHOD'] == 'GET'){
	handleGet($bits);
}else if($_SERVER['REQUEST_METHOD'] == 'PUT'){
	handlePut($bits);
}

function handleGet($bits){

	$storageID = $bits[0];
	$guid = $bits[1];
	$ts = $bits[2];
	$device = $bits[3];

	$srcPath = "/mnt/storage";
	$dsPath = "$srcPath/$storageID/$guid";

	// block == 5
	// /4/0ee224ee-2851-498f-3e9a-dc6fcaa7ea40/blocks/fff/fffb2937d95864d8e1b52d153d7e1b3f

	$len = count($bits);
	if($len == 4 && str_contains(basename(end($bits)), "hcl" ) ){		
		$getPath ="$dsPath/$ts/$device";
		logme("Sending base path: $getPath");		
		//echo "Sending $getPath";
		sendFile($getPath);
	}else if($len == 4   ){		
		$getPath ="$dsPath/$ts";
		logme("Sending base path: $getPath");
		// Perhaps we mount and unmount the DS device here?
		sendDevice($getPath, $device);							// we only compress the whole device stream
	}else if ($len == 5 && $ts == "blocks"){		
		$getPath ="$dsPath/$ts/$device/". $bits[4];
		sendFile($getPath);										// blocks are already compressed		
	}
	
	// hcl == 4
	// /4/0ee224ee-2851-498f-3e9a-dc6fcaa7ea40/1719937638/0.hcl	

	//$cmd = "/home/alike/app/scripts/qdsAdmin mount $storageID $guid";
	//logme("Mounting QDS: Running: $cmd");
	//$res = shell_exec($cmd);
	//logme("Mounting res: $res");
	

	//$cmd = "/home/alike/app/scripts/qdsAdmin unmount $dsPath";
	//logme("unmounting QDS: Running: $cmd");
	//$res = shell_exec($cmd);
	//logme("Unmounting res: $res");
	exit();
}

function sendFile($block){
//	header('Content-Encoding: zstd');
	$outputStream = fopen('php://output', 'wb');
	$chunk = file_get_contents($block);
	$res = fwrite($outputStream, $chunk);
	fclose($outputStream);
}

function sendBlock($block){
	header('Content-Encoding: zstd');
	$outputStream = fopen('php://output', 'wb');
	$chunk = file_get_contents($block);
	$compressed = zstd_compress($chunk, 6);
	$res = fwrite($outputStream, $compressed);
	fclose($outputStream);
}

function sendDevice($basePath, $devNum){
	header('Content-Encoding: zstd');

	$outputStream = fopen('php://output', 'wb');
//	$zstream = gzencode('', 9, FORCE_GZIP);
	$ms = 0;

//	/mnt/storage/1/guid/ts/blocks/
	$hclFile = "$basePath/".$devNum.".hcl";

	echo "Reading hcl: $hclFile";
	logme("Reading hcl: $hclFile");

	$totComp=0;
	$totOrig=0;

	foreach(file($hclFile) as $line) {
	    // do stuff here
		$line = trim($line);
		if($line == "CODA"){
			logme("Exiting, CODA found");
			continue;
		}
		$block = "$basePath/blocks/" . substr($line, 0, 3) ."/$line";
		$chunk = file_get_contents($block);
		$totOrig += strlen($chunk);
$st = microtime(true);
		//$compressed = gzencode($chunk, 9, FORCE_GZIP);
		$compressed = zstd_compress($chunk, 6);
$end = microtime(true);
$ms += $end - $st;
		$totComp += strlen($compressed);
		$res = fwrite($outputStream, $compressed);
		logme("Writing $block to stream. Size: ". strlen($chunk). " Res: $res");
	}
	fclose($outputStream);

	logme("Spend $ms ms compressing ");
	$saved = $totOrig - $totComp;
	logme("Read $totOrig vs Sent $totComp (Saved $saved) ");
}


function handlePut($bits){
	$parts = explode('=', $bits[1]);
	$outFile = "/dev/null";
	$isBlock = false;

	$storageID = $bits[0];
	$guid = $bits[1];
	$type = $bits[2];
	$thing = $bits[3];

	$srcPath = "/mnt/storage";
	$dsPath = "$srcPath/$storageID/$guid";

	if(!is_dir($dsPath)){
		logme("Requested PUT path doesn't exist: $dsPath");
		http_response_code(401);
		return;
	}

	if($type == "block"){
		$outFile = "$dsPath/blocks/" . substr($thing, 0, 3) ."/$thing";
		$isBlock = true;
	}else if($type == "file"){
		$outFile = "$dsPath/$thing";
		if(count($bits) > 4){
			exec("mkdir -p $outFile");
			$outFile .= "/$bits[4]";
		}
	}

	// only skip writing blocks?
	if(file_exists($outFile) && $isBlock){
		logme("Got a put for a file we already have ($outFile)");
		http_response_code(208);	// don't overwrite existing files?
		return;
	}


	// Read the raw POST data
	$rawData = file_get_contents('php://input');

	// safety check- make sure we received all the bytes they wanted to send
	$claimedSent = $_SERVER['CONTENT_LENGTH'] ?? 0;
	$sentSize = strlen($rawData);
	if ($sentSize !== (int) $claimedSent) {
		http_response_code(501);
		logme("Did not receive all bytes: $sentSize vs $claimedSent.  Discarding");
		exit(1);
	}

	logme("Received PUT for $thing (size: $sentSize)");

	$act = $rawData;	
	//$act = gzuncompress($rawData);

	//$decompress = false;
	$doCheck = true;
	//if($isBlock == false){ $decompress = true; }

	//if($decompress){
	//	$act = zstd_uncompress($rawData);
	//}

	if($isBlock && $doCheck){
		$testPrint = "";
		//if($decompress){ $testPrint = md5($act); }	// we've already decompressed it
		//else{ 
			$tmp = zstd_uncompress($rawData);
			$testPrint = md5($tmp); 
		//}
		if($testPrint != $thing){
		    http_response_code(505);
		    logme("MD5 mismatch.  Given $thing, got $testPrint.  Discarding");
			exit(1);
		}
	}

	$res =file_put_contents($outFile, $act);
	if($res == $sentSize){
		logme("Wrote file $outFile ($res bytes)");
	}else if($res === false){
		 $error = error_get_last();
		http_response_code(504);
		logme("Failed to write block $outFile.  Err: ".$error["message"]);
	}
}

function handleAuth($authHeader){
	$apiReal = getSetting("apiKey");
	$apiReal = empty($apiReal) ? 'API' : $apiReal;

	$apiKey = $authHeader ?? '';
	if($apiKey == ''){
		echo json_encode(["message" => "No authentication provided"]);
		exit();
	}
	if ($apiKey !== $apiReal) {
		http_response_code(401);
		echo json_encode(["message" => "Unauthorized"]);
		exit();
	}
}

function logme($msg){
	file_put_contents("/tmp/dumb.log", $msg ."\n", FILE_APPEND);
}

?>
