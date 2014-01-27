<?php
/**
 * ObjectStorageController implementation for the Google Cloud Storage (GCS) 
 * API
 */
class GoogleObjectStorageController extends ObjectStorageController {
  // default API endpoint for GCS
  const DEFAULT_GOOGLE_ENDPOINT = 'storage.googleapis.com';
  const DEFAULT_REGION = 'US';
  const SIGNATURE_DATE_FORMAT = 'D, d M Y H:i:s O';
  const CONTAINERS_GLOBAL = 'US,EU';
  const CONTAINERS_REGIONAL = 'US-EAST1,US-EAST2,US-EAST3,US-CENTRAL1,US-CENTRAL2,US-WEST1';
  
  // api endpoint url
  private $api_url;
  // is api_region a regional container
  private $api_region_regional = FALSE;
  
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
    if ($result = $this->curl(array($request))) {
      $success = $result['status'][0] == 200 || $result['status'][0] == 404;
      EncodingUtil::log(sprintf('GET Service request completed - status %d. Authentication was%s successful', $result['status'][0], $success ? '' : ' not'), 'GoogleObjectStorageController::authenticate', __LINE__);
    }
    else EncodingUtil::log(sprintf('GET Service request failed'), 'GoogleObjectStorageController::authenticate', __LINE__, TRUE);
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
    if ($result = $this->curl(array($request))) {
      $exists = $result['status'][0] == 200;
      EncodingUtil::log(sprintf('HEAD Bucket request completed - status %d. Bucket %s does%s exist', $result['status'][0], $container, $exists ? '' : ' not'), 'GoogleObjectStorageController::containerExists', __LINE__);
    }
    else EncodingUtil::log(sprintf('HEAD Bucket request failed'), 'GoogleObjectStorageController::containerExists', __LINE__, TRUE);
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
    if ($result = $this->curl(array($request))) {
      $deleted = $result['status'][0] == 204;
      EncodingUtil::log(sprintf('DELETE Object request completed - status %d', $result['status'][0], $container), 'GoogleObjectStorageController::deleteObject', __LINE__);
    }
    else EncodingUtil::log(sprintf('DELETE Object request failed'), 'GoogleObjectStorageController::deleteObject', __LINE__, TRUE);
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
    if ($result = $this->curl(array($request))) {
      if (($exists = $result['status'][0] == 200) && isset($result['response'][0]['content-length'])) $size = $result['response'][0]['content-length'];
      EncodingUtil::log(sprintf('HEAD Object request completed - status %d. Object %s/%s does%s exist. %s', $result['status'][0], $container, $object, $exists ? '' : ' not', $size ? 'Size is ' . $size . ' bytes' : ''), 'GoogleObjectStorageController::getObjectSize', __LINE__);
    }
    else EncodingUtil::log(sprintf('HEAD Object request failed'), 'GoogleObjectStorageController::getObjectSize', __LINE__, TRUE);
    return $size;
  }
  
  /**
   * Returns the GCS API URL to use for the specified $container and $object
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
    
    if (!$this->api_region) $this->api_region = self::DEFAULT_REGION;
    $this->api_region_regional = in_array(trim(strtoupper($this->api_region)), explode(',', self::CONTAINERS_REGIONAL));
		$this->api_url = 'https://' . self::DEFAULT_GOOGLE_ENDPOINT;
    EncodingUtil::log(sprintf('Set GCS API URL to %s', $this->api_url), 'GoogleObjectStorageController::init', __LINE__);
    
    return TRUE;
  }
  
  /**
   * returns an array corresponding with the names of the objects in $container
   * @param string $container the container to check 
   * @param string $prefix optional directory style prefix to limit results
   * (e.g. '/images/gifs')
   * @return array
   */
  protected function listContainer($container, $prefix=NULL) {
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT));
    $params = NULL;
    if ($prefix) $params = array('prefix' => $prefix);
    $headers['Authorization'] = $this->sign('GET', $headers, $container, NULL, $params);
    $request = array('method' => 'GET', 'url' => $this->getUrl($container, NULL, $params), 'headers' => $headers);
    $objects = NULL;
    if ($result = EncodingUtil::curl(array($request), TRUE)) {
      if ($result['status'][0] == 200) {
        EncodingUtil::log(sprintf('GET Bucket %s request successful', $container), 'GoogleObjectStorageController::listContainer', __LINE__);
        $objects = array();
        if (preg_match_all('/key\>([^<]+)\<\/key/i', $result['body'][0], $m)) {
          $objects = $m[1];
          EncodingUtil::log(sprintf('Bucket %s contains %d objects: %s', $container, count($objects), implode(', ', $objects)), 'GoogleObjectStorageController::listContainer', __LINE__);
        }
        else EncodingUtil::log(sprintf('Bucket %s is empty with prefix %s', $container, $prefix), 'GoogleObjectStorageController::listContainer', __LINE__);
      }
      else EncodingUtil::log(sprintf('GET Bucket %s request failed - status %d', $container, $result['status'][0]), 'GoogleObjectStorageController::listContainer', __LINE__, TRUE);
    }
    else EncodingUtil::log(sprintf('GET Bucket request failed'), 'GoogleObjectStorageController::listContainer', __LINE__, TRUE);
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
    if ($result = $this->curl(array($request))) {
      $exists = $result['status'][0] == 200;
      EncodingUtil::log(sprintf('HEAD Object request completed - status %d. Object %s/%s does%s exist', $result['status'][0], $container, $object, $exists ? '' : ' not'), 'GoogleObjectStorageController::objectExists', __LINE__);
    }
    else EncodingUtil::log(sprintf('HEAD Object request failed'), 'GoogleObjectStorageController::objectExists', __LINE__, TRUE);
    return $exists;
  }
  
  /**
   * returns an authorization signature for the parameters specified
   * @param string $method the http method
   * @param array $headers http headers
   * @param string $container optional container
   * @param string $object optional object to create the signature for
   * @return string
   */
  private function sign($method, $headers, $container=NULL, $object=NULL) {
    // add goog headers to signature
    $goog_headers = array();
    foreach($headers as $key => $val) {
      if (preg_match('/^x-goog/', $key)) $goog_headers[strtolower($key)] = $val;
    }
    ksort($goog_headers);
    $goog_string = '';
    foreach($goog_headers as $key => $val) $goog_string .= $key . ':' . trim($val) . "\n";
    
    $uri = '';
    if ($object) $uri = $container . '/' . str_replace('%2F', '/', urlencode($object));
    else if ($method == 'PUT' && !$object) $uri = $container;
    else if ($container) $uri = $container . '/';
    $string = sprintf("%s\n\n%s\n%s\n%s/%s", 
                      strtoupper($method),
                      isset($headers['content-type']) ? $headers['content-type'] : '',
                      $headers['date'], 
                      $goog_string,
                      $uri);
    EncodingUtil::log(sprintf('Signing string %s', str_replace("\n", '\n', $string)), 'GoogleObjectStorageController::sign', __LINE__);
		$signature = base64_encode(extension_loaded('hash') ? hash_hmac('sha1', $string, $this->api_secret, TRUE) : pack('H*', sha1((str_pad($this->api_secret, 64, chr(0x00)) ^ (str_repeat(chr(0x5c), 64))) . pack('H*', sha1((str_pad($this->api_secret, 64, chr(0x00)) ^ (str_repeat(chr(0x36), 64))) . $string)))));
		return sprintf('GOOG1 %s:%s', $this->api_key, $signature);
  }
  
}
?>
