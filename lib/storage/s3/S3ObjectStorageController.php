<?php
/**
 * ObjectStorageController implementation for S3
 */
class S3ObjectStorageController extends ObjectStorageController {
  // default API endpoint for S3
  const DEFAULT_S3_ENDPOINT = 's3.amazonaws.com';
  const DEFAULT_S3_REGION = 'us-east-1';
  const SIGNATURE_DATE_FORMAT = 'D, d M Y H:i:s T';
  
  // complete API endpoint URL (e.g. https://s3.amazonaws.com)
  private $api_url;
  
  /**
   * invoked once during validation. Should return TRUE if authentication is 
   * successful, FALSE otherwise. this method should reference the instance
   * attributes $api_key, $api_secret and $api_region as 
   * necessary to complete the authentication
   * @return boolean
   */
  protected function authenticate() {
    // test authentication by listing containers
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT));
    $headers['Authorization'] = $this->sign('GET', $headers);
    $request = array('method' => 'GET', 'url' => $this->getUrl(), 'headers' => $headers);
    $success = NULL;
    if ($result = EncodingUtil::curl(array($request))) {
      $success = $result['status'][0] == 200 || $result['status'][0] == 404;
      EncodingUtil::log(sprintf('GET Service request completed - status %d. Authentication was%s successful', $result['status'][0], $success ? '' : ' not'), 'S3ObjectStorageController::authenticate', __LINE__);
    }
    else EncodingUtil::log(sprintf('GET Service request failed'), 'S3ObjectStorageController::authenticate', __LINE__, TRUE);
    return $success;
  }
  
  /**
   * returns TRUE if $container exists, FALSE otherwise. return NULL on 
   * error
   * @param string $container the container to check 
   * @return boolean
   */
  public function containerExists($container) {
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT));
    $headers['Authorization'] = $this->sign('HEAD', $headers, $container);
    $request = array('method' => 'HEAD', 'url' => $this->getUrl($container), 'headers' => $headers);
    $exists = NULL;
    if ($result = EncodingUtil::curl(array($request))) {
      $exists = $result['status'][0] == 200;
      EncodingUtil::log(sprintf('HEAD Bucket request completed - status %d. Bucket %s does%s exist', $result['status'][0], $container, $exists ? '' : ' not'), 'S3ObjectStorageController::containerExists', __LINE__);
    }
    else EncodingUtil::log(sprintf('HEAD Bucket request failed'), 'S3ObjectStorageController::containerExists', __LINE__, TRUE);
    return $exists;
  }
  
  /**
   * deletes $object. returns TRUE on success, FALSE on failure
   * @param string $container the object container
   * @param string $object name of the object to delete
   * @return boolean
   */
  public function deleteObject($container, $object) {
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT));
    $headers['Authorization'] = $this->sign('DELETE', $headers, $container, $object);
    $request = array('method' => 'DELETE', 'url' => $this->getUrl($container, $object), 'headers' => $headers);
    $deleted = NULL;
    if ($result = EncodingUtil::curl(array($request))) {
      $deleted = $result['status'][0] == 204;
      EncodingUtil::log(sprintf('DELETE Object request completed - status %d', $result['status'][0], $container), 'S3ObjectStorageController::deleteObject', __LINE__);
    }
    else EncodingUtil::log(sprintf('DELETE Object request failed'), 'S3ObjectStorageController::deleteObject', __LINE__, TRUE);
    return $deleted;
  }
  
  /**
   * returns the size of $object in bytes. return NULL on error
   * @param string $container the object container
   * @param string $object name of the object
   * @return int
   */
  public function getObjectSize($container, $object) {
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT));
    $headers['Authorization'] = $this->sign('HEAD', $headers, $container, $object);
    $request = array('method' => 'HEAD', 'url' => $this->getUrl($container, $object), 'headers' => $headers);
    $size = NULL;
    if ($result = EncodingUtil::curl(array($request))) {
      if (($exists = $result['status'][0] == 200) && isset($result['response'][0]['content-length'])) $size = $result['response'][0]['content-length'];
      EncodingUtil::log(sprintf('HEAD Object request completed - status %d. Object %s/%s does%s exist. %s', $result['status'][0], $container, $object, $exists ? '' : ' not', $size ? 'Size is ' . $size . ' bytes' : ''), 'S3ObjectStorageController::getObjectSize', __LINE__);
    }
    else EncodingUtil::log(sprintf('HEAD Object request failed'), 'S3ObjectStorageController::getObjectSize', __LINE__, TRUE);
    return $size;
  }
  
  /**
   * returns the URL to the $object specified (in the designated container)
   * @param string $object name of the object
   * @param boolean $auth whether or not to include auth parameters using 
   * standard http auth method (e.g.) http://[user]:[pass]@[url]
   * @return string
   */
  public function getObjectUrl($object, $auth=FALSE) {
    $url = 'https://' . self::DEFAULT_S3_ENDPOINT;
    $container = $this->getContainer();
    $dns_containers = TRUE;
    if ($dns_containers) $url = str_replace('://', '://' . $container . '.', $url);
    $url = sprintf('%s%s%s', $url, $dns_containers ? '' : '/' . $container, $object ? '/' . str_replace('%2F', '/', urlencode($object)) : '');
    if ($auth) $url = str_replace('://', sprintf('://%s:%s@', urlencode($this->api_key), urlencode($this->api_secret)), $url);
    return $url;
  }
  
  /**
   * Returns the S3 API URL to use for the specified $container and $object
   * @param string $container the container to return the URL for
   * @param string $object optional object to include in the URL
   * @param array $params optional URL parameters
   * @param boolean $dnsContainers may be used to override $this->dns_containers
   * @return string
   */
  private function getUrl($container=NULL, $object=NULL, $params=NULL, $dnsContainers=NULL) {
    $url = $this->api_url;
    if ($container) {
      $dns_containers = $dnsContainers !== NULL ? $dnsContainers : TRUE;
      if ($dns_containers) $url = str_replace('://', '://' . $container . '.', $url);
      $url = sprintf('%s%s%s', $url, $dns_containers ? '' : '/' . $container, $object ? '/' . str_replace('%2F', '/', urlencode($object)) : '');
      if (is_array($params)) {
        foreach(array_keys($params) as $i => $param) {
          $url .= ($i ? '&' : '?') . $param . ($params[$param] ? '=' . $params[$param] : '');
        }
      }
    }
    return $url;
  }
  
  /**
   * may be used to perform pre-usage initialization. return TRUE on success, 
   * FALSE on failure
   * @return boolean
   */
  protected function init() {
    
    // determine region
    if (!$this->api_region) $this->api_region = self::DEFAULT_S3_REGION;
		foreach(explode("\n", file_get_contents(dirname(__FILE__) . '/region-mappings.ini')) as $line) {
			if (substr(trim($line), 0, 1) == '#') continue;
			if (preg_match('/^([^=]+)=(.*)$/', trim($line), $m)) {
				if (in_array($this->api_region, explode(',', $m[1]))) {
					$this->api_region = trim($m[2]);
					break;
				}
			}
		}
    EncodingUtil::log(sprintf('Set S3 API region to %s', $this->api_region), 'S3ObjectStorageController::init', __LINE__);
    
    // determine endpoint
		if ($this->api_region) {
			foreach(explode("\n", file_get_contents(dirname(__FILE__) . '/region-endpoints.ini')) as $line) {
				if (substr(trim($line), 0, 1) == '#') continue;
				if (preg_match('/^([^=]+)=(.*)$/', trim($line), $m) && $m[1] == $this->api_region) $this->api_endpoint = trim($m[2]);
			}
		}
		if (!$this->api_endpoint) $this->api_endpoint = self::DEFAULT_S3_ENDPOINT;
		$this->api_url = 'https://' . $this->api_endpoint;
    EncodingUtil::log(sprintf('Set S3 API URL to %s', $this->api_url), 'S3ObjectStorageController::init', __LINE__);
    $this->api_ssl = preg_match('/^https/', $this->api_url) ? TRUE : FALSE;

    return TRUE;
  }
  
  /**
   * returns an array corresponding with the names of the objects in $container
   * @param string $container the container to check 
   * @param string $prefix optional directory style prefix to limit results
   * (e.g. '/images/gifs')
   * @param string $marker used for follow on requests for containers with 
   * more than the max allowed in a single request
   * @return array
   */
  protected function listContainer($container, $prefix=NULL, $marker=NULL) {
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT));
    $params = array();
    if ($prefix) $params['prefix'] = $prefix;
    if ($marker) $params['marker'] = $marker;
    $headers['Authorization'] = $this->sign('GET', $headers, $container, NULL, $params);
    $request = array('method' => 'GET', 'url' => $this->getUrl($container, NULL, $params), 'headers' => $headers);
    $objects = NULL;
    if ($result = EncodingUtil::curl(array($request), TRUE)) {
      if ($result['status'][0] == 200) {
        EncodingUtil::log(sprintf('GET Bucket %s request successful', $container), 'S3ObjectStorageController::listContainer', __LINE__);
        $objects = array();
        if (preg_match_all('/key\>([^<]+)\<\/key/i', $result['body'][0], $m)) {
          $objects = $m[1];
          // truncated request - initiate follow on using marker
          if (preg_match('/istruncated\>true\<\/istruncated/i', $result['body'][0])) {
            $nextMarker = preg_match('/nextmarker\>([^<]+)\<\/nextmarker/i', $result['body'][0], $m) ? $m[1] : $objects[count($objects) - 1];
            EncodingUtil::log(sprintf('GET Bucket %s results are truncated - initiating follow on request using next marker %s', $container, $nextMarker), 'S3ObjectStorageController::listContainer', __LINE__);
            if ($more_objects = $this->listContainer($container, $prefix, $nextMarker)) {
              EncodingUtil::log(sprintf('GET Bucket %s follow on request successful - adding %d objects', $container, count($more_objects)), 'S3ObjectStorageController::listContainer', __LINE__);
              foreach($more_objects as $obj) if ($obj != $marker) $objects[] = $obj;
            }
            else EncodingUtil::log(sprintf('GET Bucket %s follow on request failed', $container), 'S3ObjectStorageController::listContainer', __LINE__, TRUE);
          }
          if (!$marker) EncodingUtil::log(sprintf('Bucket %s contains %d objects: %s', $container, count($objects)), 'S3ObjectStorageController::listContainer', __LINE__);
        }
        else EncodingUtil::log(sprintf('Bucket %s is empty with prefix %s', $container, $prefix), 'S3ObjectStorageController::listContainer', __LINE__);
      }
      else EncodingUtil::log(sprintf('GET Bucket %s request failed - status %d', $container, $result['status'][0]), 'S3ObjectStorageController::listContainer', __LINE__, TRUE);
    }
    else EncodingUtil::log(sprintf('GET Bucket request failed'), 'S3ObjectStorageController::listContainer', __LINE__, TRUE);
    return $objects;
  }
  
  /**
   * returns TRUE if the object identified by $name exists in $container. 
   * return NULL on error
   * @param string $container the container to check
   * @param string $object the name of the object
   * @return boolean
   */
  public function objectExists($container, $object) {
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT));
    $headers['Authorization'] = $this->sign('HEAD', $headers, $container, $object);
    $request = array('method' => 'HEAD', 'url' => $this->getUrl($container, $object), 'headers' => $headers);
    $exists = NULL;
    if ($result = EncodingUtil::curl(array($request))) {
      $exists = $result['status'][0] == 200;
      EncodingUtil::log(sprintf('HEAD Object request completed - status %d. Object %s/%s does%s exist', $result['status'][0], $container, $object, $exists ? '' : ' not'), 'S3ObjectStorageController::objectExists', __LINE__);
    }
    else EncodingUtil::log(sprintf('HEAD Object request failed'), 'S3ObjectStorageController::objectExists', __LINE__, TRUE);
    return $exists;
  }
  
  /**
   * returns an authorization signature for the parameters specified
   * @param string $method the http method
   * @param array $headers http headers
   * @param string $container optional container
   * @param string $object optional object to create the signature for
   * @param array $params optional URL parameters
   * @return string
   */
  private function sign($method, $headers, $container=NULL, $object=NULL, $params=NULL) {
    // add amz headers to signature
    $amz_headers = array();
    foreach($headers as $key => $val) {
      if (preg_match('/^x-amz/', $key)) $amz_headers[strtolower($key)] = $val;
    }
    ksort($amz_headers);
    $amz_string = '';
    foreach($amz_headers as $key => $val) $amz_string .= $key . ':' . trim($val) . "\n";
    
    $uri = '';
    if ($object) $uri = $container . '/' . str_replace('%2F', '/', urlencode($object));
    else if ($method == 'PUT' && !$object) $uri = $container;
    else if ($container) $uri = $container . '/';
    $string = sprintf("%s\n\n%s\n%s\n%s/%s", 
                      strtoupper($method),
                      isset($headers['content-type']) ? $headers['content-type'] : '',
                      $headers['date'], 
                      $amz_string,
                      $uri);
    if ($params) {
      ksort($params);
      $started = FALSE;
      foreach($params as $key => $val) {
        if (in_array($key, array('acl', 'lifecycle', 'location', 'logging', 'notification', 'partNumber', 'policy', 'requestPayment', 'torrent', 'uploadId', 'uploads', 'versionId', 'versioning', 'versions', 'website'))) {
          $string .= ($started ? '&' : '?') . $key . ($val ? '=' . $val : '');
          $started = TRUE;
        }
      }
    }
    EncodingUtil::log(sprintf('Signing string %s', str_replace("\n", '\n', $string)), 'S3ObjectStorageController::sign', __LINE__);
		$signature = base64_encode(extension_loaded('hash') ? hash_hmac('sha1', $string, $this->api_secret, TRUE) : pack('H*', sha1((str_pad($this->api_secret, 64, chr(0x00)) ^ (str_repeat(chr(0x5c), 64))) . pack('H*', sha1((str_pad($this->api_secret, 64, chr(0x00)) ^ (str_repeat(chr(0x36), 64))) . $string)))));
		return sprintf('AWS %s:%s', $this->api_key, $signature);
  }
  
}
?>
