<?php
/**
 * Utility methods
 */
class EncodingUtil {
  
  // default values for runtime parameters
  const DEFAULT_ROUND_PRECISION = 4;
  
  /**
   * invokes 1 or more http requests using curl, waits until they are 
   * completed, records the stats, and returns the associated results. Return
   * value is a hash containing the following keys:
   *   urls:     ordered array of URLs
   *   request:  ordered array of request headers (lowercase keys)
   *   response: ordered array of response headers (lowercase keys)
   *   results:  ordered array of curl result values - includes the following:
   *             speed:              transfer rate (bytes/sec)
   *             time:               total time for the operation
   *             transfer:           total bytes transferred
   *             url:                actual URL used
   *   status:   ordered array of status codes
   *   lowest_status: the lowest status code returned
   *   highest_status: the highest status code returned
   *   body:     response body (only included when $retBody is TRUE)
   *   form:     optional hash representing form fields that should be included
   *             the request
   * returns NULL if any of the curl commands fail
   * @param array $requests array defining the http requests to invoke. Each 
   * element in this array is a hash with the following possible keys:
   *   method:  http method (default is GET)
   *   headers: hash defining http headers to append
   *   url:     the URL
   *   input:   optional command to pipe into the curl process as the body
   *   body:    optional string or file to pipe into the curl process as the 
   *            body
   *   range:   optional request byte range
   * @param boolean $retBody if TRUE, the response body will be included in the 
   * return
   * @return array
   */
  public static function curl($requests, $retBody=FALSE) {
    global $bm_param_debug;
    static $encoding_concurrent_requests;
    static $max_api_requests_sec;
    static $last_api_time;
    static $last_api_time_requests;
    
    if (!isset($bm_param_debug)) $bm_param_debug = getenv('bm_param_debug') == '1';
    if (!isset($encoding_concurrent_requests)) $encoding_concurrent_requests = getenv('bm_param_concurrent_requests')*1;
    if (!$encoding_concurrent_requests) $encoding_concurrent_requests = EncodingController::DEFAULT_CONCURRENT_REQUESTS;
    if (!isset($max_api_requests_sec)) {
      $max_api_requests_sec = getenv('bm_param_max_api_requests_sec');
      if (!is_numeric($max_api_requests_sec) || $max_api_requests_sec < 1) $max_api_requests_sec = FALSE;
    }
    if ($max_api_requests_sec && $last_api_time >= (time() - 1) && $last_api_time_requests >= $max_api_requests_sec) {
      self::log(sprintf('Sleeping 1 second because max API requests %d would be exceeded', $max_api_requests_sec), 'EncodingUtil::curl', __LINE__);
      sleep(1);
    }
    else if ($last_api_time < time()) {
      $last_api_time_requests = 0;
    }
    
    $result = array('urls' => array(), 'request' => array(), 'response' => array(), 'results' => array(), 'status' => array(), 'lowest_status' => 0, 'highest_status' => 0);
    $fstart = microtime(TRUE);
    $script = sprintf('%s/%s', getenv('bm_run_dir'), 'curl_script_' . rand());
    $fp = fopen($script, 'w');
    fwrite($fp, "#!/bin/sh\n");
    $ifiles = array();
    $ofiles = array();
    $bfiles = array();
    if ($retBody) $result['body'] = array();
    $request_num = 0;
    foreach($requests as $i => $request) {
      $request_num++;
      if (isset($request['body'])) {
        if (file_exists($request['body'])) $file = $request['body'];
        else {
          $ifiles[$i] = sprintf('%s/%s', getenv('bm_run_dir'), 'curl_input_' . rand());
          $f = fopen($ifiles[$i], 'w');
          fwrite($f, $request['body']);
          fclose($f); 
          $file = $ifiles[$i];
        }
        $request['input'] = 'cat ' . $file;
        $request['headers']['content-length'] = filesize($file);
      }
      if (!isset($request['headers'])) $request['headers'] = array();
      $method = isset($request['method']) ? strtoupper($request['method']) : 'GET';
      $body = '/dev/null';
      if ($retBody) {
        $bfiles[$i] = sprintf('%s/%s', getenv('bm_run_dir'), 'curl_body_' . rand());
        $body = $bfiles[$i];
      }
      $cmd = (isset($request['input']) ? $request['input'] . ' | curl --data-binary @-' : 'curl') . ($method == 'HEAD' ? ' -I' : '') . ' -s -o ' . $body . ' -D - -X ' . $method;
      $result['request'][$i] = $request['headers'];
      foreach($request['headers'] as $header => $val) $cmd .= sprintf(' -H "%s: %s"', $header, $val);
      if (isset($request['range'])) $cmd .= ' -r ' . $request['range'];
      $result['urls'][$i] = $request['url'];
      if (isset($request['form']) && is_array($request['form'])) {
        foreach($request['form'] as $field => $val) {
          $cmd .= sprintf(" --form-string '%s=%s'", $field, str_replace("'", "\'", $val));
        }
      }
      $cmd .= sprintf(' "%s"', $request['url']);
      if ($bm_param_debug) self::log(sprintf('Added curl command: %s', $cmd), 'EncodingUtil::curl', __LINE__);
      $ofiles[$i] = sprintf('%s/%s', getenv('bm_run_dir'), 'curl_output_' . rand());
      
      // max_api_requests_sec
      if ($max_api_requests_sec && ($request_num % $max_api_requests_sec) == 0) {
        self::log(sprintf('Number of concurrent requests %d exceeds max API requests/second %d. Added 1 second sleep at %d', count($requests), $max_api_requests_sec, $request_num), 'EncodingUtil::curl', __LINE__);
        fwrite($fp, sprintf("sleep 1\n"));
        $last_api_time_requests = 0;
      }
      else $last_api_time_requests++;
      
      fwrite($fp, sprintf("%s > %s 2>&1 &\n", $cmd, $ofiles[$i]));
      
      // max concurrent requests
      if (($request_num % $encoding_concurrent_requests) == 0) {
        self::log(sprintf('Number of concurrent requests %d exceeds max allowed %d. Added wait at %d', count($requests), $encoding_concurrent_requests, $request_num), 'EncodingUtil::curl', __LINE__);
        fwrite($fp, sprintf("wait\n"));
      }
    }
    fwrite($fp, "wait\n");
    fclose($fp);
    exec(sprintf('chmod 755 %s', $script));
    self::log(sprintf('Created script %s containing %d curl commands. Executing...', $script, count($requests)), 'EncodingUtil::curl', __LINE__);
    $start = microtime(TRUE);
    exec($script);
    $curl_time = microtime(TRUE) - $start;
    self::log(sprintf('Execution complete - retrieving results', $script, count($requests)), 'EncodingUtil::curl', __LINE__);
    foreach(array_keys($requests) as $i) {
      foreach(file($ofiles[$i]) as $line) {
        // status code
        if (preg_match('/HTTP[\S]+\s+([0-9]+)\s/', $line, $m)) {
          $status = $m[1]*1;
          $result['status'][$i] = $status;
          if ($result['lowest_status'] === 0 || $status < $result['lowest_status']) $result['lowest_status'] = $status;
          if ($status > $result['highest_status']) $result['highest_status'] = $status;
        }
        // response header
        else if (preg_match('/^([^:]+):\s+"?([^"]+)"?$/', trim($line), $m)) $result['response'][$i][trim(strtolower($m[1]))] = $m[2];
        // result value
        else if (preg_match('/^([^=]+)=(.*)$/', trim($line), $m)) $result['results'][$i][trim(strtolower($m[1]))] = $m[2];
        // body
        if (isset($bfiles[$i]) && file_exists($bfiles[$i])) {
          $result['body'][$i] = file_get_contents($bfiles[$i]);
          unlink($bfiles[$i]);
        }
      }
      unlink($ofiles[$i]);
    }
    foreach($ifiles as $ifile) unlink($ifile);
    unlink($script);
    
    self::log(sprintf('Results processed - lowest status %d; highest status %d', $result['lowest_status'], $result['highest_status']), 'EncodingUtil::curl', __LINE__);    
    if (!$result['highest_status']) {
      self::log(sprintf('curl execution failed'), 'EncodingUtil::curl', __LINE__, TRUE);
      $result = NULL;
    }
    else if ($bm_param_debug) foreach(array_keys($requests) as $i) self::log(sprintf('  %s => %d', $result['urls'][$i], $result['status'][$i]), 'EncodingUtil::curl', __LINE__);
    
    $last_api_time = time();

    return $result;
  }
  
  /**
   * returns TRUE if $file is audio media (i.e. not video)
   * @param string $file the media file to evaluate - can also be just a file
   * extension
   * @return boolean
   */
  public static function isAudio($file) {
    return preg_match('/mp3$/i', $file) || preg_match('/aac$/i', $file) || preg_match('/wav$/i', $file) || preg_match('/m4a$/i', $file);
  }

  /**
   * prints a log message - may be used by implementations to log informational
   * and error messages. Informational messages (when $error=FALSE) are only 
   * logged when the debug runtime parameter is set. Error messages are always 
   * logged
   * @param string $msg the message to output (REQUIRED)
   * @param string $source the source of the message
   * @param int $line an optional line number
   * @param boolean $error is this an error message
   * @param string $source1 secondary source
   * @param int $line1 secondary line number
   * @return void
   */
  public static function log($msg, $source=NULL, $line=NULL, $error=FALSE, $source1=NULL, $line1=NULL) {
    global $bm_param_debug;
    if (!isset($bm_param_debug)) $bm_param_debug = getenv('bm_param_debug') == '1';
    if ($msg && ($bm_param_debug || $error)) {
      // remove passwords and secrets
      $msg = preg_replace('/Key:\s+([^"]+)/', 'Key: xxx', $msg);
      $msg = preg_replace('/Token:\s+([^"]+)/', 'Token: xxx', $msg);
      $msg = preg_replace('/Authorization:\s+([^"]+)/', 'Authorization: xxx', $msg);
      foreach(array(getenv('bm_param_service_key'), getenv('bm_param_service_secret'), getenv('bm_param_storage_key'), getenv('bm_param_storage_secret')) as $secret) {
        if ($secret) {
          $msg = str_replace($secret, 'xxx', $msg);
          $msg = str_replace(urlencode($secret), 'xxx', $msg);
        }
      }
      
    	global $base_error_level;
    	$source = basename($source);
    	if ($source1) $source1 = basename($source1);
    	$exec_time = self::runtime();
    	// avoid timezone errors
    	error_reporting(E_ALL ^ E_NOTICE ^ E_WARNING);
    	$timestamp = date('m/d/Y H:i:s T');
    	error_reporting($base_error_level);
    	printf("%-24s %-12s %-12s %s\n", $timestamp, $exec_time . 's', 
    				 $source ? str_replace('.php', '', $source) . ($line ? ':' . $line : '') : '', 
    				 ($error ? 'ERROR - ' : '') . $msg . 
    				 ($source1 ? ' [' . str_replace('.php', '', $source1) . ($line1 ? ":$line1" : '') . ']' : '')); 
    }
  }
  
  /**
   * returns the current execution time in seconds
   * @return float
   */
  public static function runtime() {
  	global $start_time;
    if (!isset($start_time)) $start_time = microtime(TRUE);
  	return round(microtime(TRUE) - $start_time, self::DEFAULT_ROUND_PRECISION);
  }
  
}
?>
