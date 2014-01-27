<?php
require_once(dirname(dirname(__FILE__)) . '/EncodingUtil.php');
/**
 * Abstract class defining and providing partial implementation of an object 
 * storage controller. This class is used to interact with a supported object
 * storage system to facilitate encode testing
 */
abstract class ObjectStorageController {
  
  /**
   * Runtime properties - set automatically following instantiation. 
   * Documentation for each is provided in the README
   */
  protected $api_key;
  protected $api_region;
  protected $api_secret;
  
  // private attributes - may not be accessed by API implementations
  private $api;
  private $container;
  
  /**
   * Returns the name of the API
   * @return string
   */
  public final function getApi() {
    return $this->api;
  }
  
  /**
   * Returns the api key
   * @return string
   */
  public final function getApiKey() {
    return $this->api_key;
  }
  
  /**
   * Returns the api region
   * @return string
   */
  public final function getApiRegion() {
    return $this->api_region;
  }
  
  /**
   * Returns the api secret
   * @return string
   */
  public final function getApiSecret() {
    return $this->api_secret;
  }
  
  /**
   * Returns the name of the object storage container
   * @return string
   */
  public final function getContainer() {
    return $this->container;
  }
  
  /**
   * returns the names of objects in the designated container. returns an 
   * array on success (empty if container has no objects or none matching 
   * $filter), NULL on error
   * @param string $filter optional object name filter - may contain '*' 
   * character to match 0 or more wildcard characters. Filtering is not 
   * case sensitive
   * @return array
   */
  public final function getContainerObjects($filter=NULL) {
    $prefix = NULL;
    if ($filter && count($pieces = explode('/', $filter)) > 1) for($i=0; $i<count($pieces) - 1; $i++) $prefix .= ($prefix ? '/' : '') . $pieces[$i];
    EncodingUtil::log(sprintf('Getting objects in container %s using prefix %s and filter %s', $this->container, $prefix, $filter), 'ObjectStorageController::getContainerObjects', __LINE__);
    if (is_array($objects = $this->listContainer($this->container, $prefix)) && $filter) {
      $regex = sprintf('/^%s$/i', str_replace('\*', '.*', str_replace('/', '\/', preg_quote($filter))));
      EncodingUtil::log(sprintf('Matching filter %s from list of %d objects using regex %s', $filter, count($objects), $regex), 'ObjectStorageController::getContainerObjects', __LINE__);
      $nobjects = array();
      foreach($objects as $object) {
        if (preg_match($regex, $object)) {
          EncodingUtil::log(sprintf('Object %s matched regex filter - adding to object list', $object), 'ObjectStorageController::getContainerObjects', __LINE__);
          $nobjects[] = $object;
        }
        else EncodingUtil::log(sprintf('Object %s does not match regex filter', $object), 'ObjectStorageController::getContainerObjects', __LINE__);
      }
      $objects = $nobjects;
    }
    // remove directories
    if ($objects) {
      $nobjects = array();
      foreach($objects as $object) {
        if (!preg_match('/\/$/', $object)) $nobjects[] = $object;
      }
      $objects = $nobjects;
    }
    return $objects;
  }
  
  /**
   * Singleton method to use in order to instantiate
   * @return ObjectStorageController
   */
  public static final function &getInstance() {
    static $_instances;
    $api = getenv('bm_param_storage_service');
    
    if (!isset($_instances[$api])) {
      $dir = dirname(__FILE__) . '/' . $api;
      if ($api && is_dir($dir)) {
        $d = dir($dir);
        $controller_file = NULL;
        while($file = $d->read()) {
          if (preg_match('/ObjectStorageController\.php$/', $file)) $controller_file = $file;
        }
        if ($controller_file) {
          require_once($dir . '/' . $controller_file);
          $controller_class = str_replace('.php', '', basename($controller_file));
          if (class_exists($controller_class)) {
            EncodingUtil::log(sprintf('Instantiating new ObjectStorageController using class %s', $controller_class), 'ObjectStorageController::getInstance', __LINE__);
            $_instances[$api] = new $controller_class();
            if (is_subclass_of($_instances[$api], 'ObjectStorageController')) {
              // set runtime parameters
              $_instances[$api]->api = $api;
              $_instances[$api]->api_key = getenv('bm_param_storage_key');
              $_instances[$api]->api_region = getenv('bm_param_storage_region');
              $_instances[$api]->api_secret = getenv('bm_param_storage_secret');
              $_instances[$api]->container = getenv('bm_param_storage_container');
              EncodingUtil::log(sprintf('Container name: %s', $_instances[$api]->container), 'ObjectStorageController::getInstance', __LINE__);
              
              EncodingUtil::log(sprintf('ObjectStorageController implementation %s for storage service %s instantiated successfully. Initiating...', $controller_class, $api), 'ObjectStorageController::getInstance', __LINE__);
              if (!$_instances[$api]->init()) {
                EncodingUtil::log(sprintf('Unable to initiate storage service - aborting test'), 'ObjectStorageController::getInstance', __LINE__, TRUE);
                $_instances[$api] = NULL;                
              }
              else if ($_instances[$api]->validate()) EncodingUtil::log(sprintf('Runtime validation successful for storage service %s', $api), 'ObjectStorageController::getInstance', __LINE__);
              else {
                EncodingUtil::log(sprintf('Runtime parameters are invalid - aborting test'), 'ObjectStorageController::getInstance', __LINE__, TRUE);
                $_instances[$api] = NULL;
              }
            }
            else EncodingUtil::log(sprintf('ObjectStorageController implementation %s for storage service %s does not extend the base class ObjectStorageController', $controller_class, $api), 'ObjectStorageController::getInstance', __LINE__, TRUE);
          }
        }
        else EncodingUtil::log(sprintf('ObjectStorageController implementation not found for storage service %s', $api), 'ObjectStorageController::getInstance', __LINE__, TRUE);
      }
      else if ($api) EncodingUtil::log(sprintf('storage_service parameter "%s" is not valid', $api), 'ObjectStorageController::getInstance', __LINE__, TRUE); 
      else EncodingUtil::log('storage_service parameter is not set', 'ObjectStorageController::getInstance', __LINE__, TRUE); 
    }
    
    if (isset($_instances[$api])) return $_instances[$api];
    else return $nl = NULL;
  }
  
  /**
   * determine the size of an object - uses a static cache to avoid repeat API
   * calls
   * @param string $name the object to return the size for
   * @return int
   */
  public final function getSize($name) {
    if (!isset($this->sizeCache)) $this->sizeCache = array();
    if (!isset($this->sizeCache[$name])) $this->sizeCache[$name] = $this->getObjectSize($this->container, $name);
    return isset($this->sizeCache[$name]) ? $this->sizeCache[$name] : NULL;
  }
  
  /**
   * Validates runtime parameters - returns TRUE on success, FALSE on failure.
   * If a validation failure occurs, the relevant error message will be logged
   */
  public final function validate() {
    if (!isset($this->validated)) {
      $this->validated = TRUE;
      
      if (!$this->authenticate()) {
        EncodingUtil::log(sprintf('Storage service authentication failed using key %s; region %s', $this->api_key, $this->api_region), 'ObjectStorageController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      else EncodingUtil::log(sprintf('Storage service authentication successful using key %s; region %s', $this->api_key, $this->api_region), 'ObjectStorageController::validate', __LINE__);
    }
    return $this->validated;
  }
  
  
  // these methods may by overriden by an API implementation
  
  /**
   * may be used to perform pre-usage initialization. return TRUE on success, 
   * FALSE on failure
   * @return boolean
   */
  protected function init() {
    return TRUE;
  }
  
  
  // these methods must be defined for each API implementation
  
  /**
   * invoked once during validation. Should return TRUE if authentication is 
   * successful, FALSE otherwise. this method may reference the optional 
   * attributes $api_key, $api_secret and $api_region as necessary to complete 
   * authentication
   * @return boolean
   */
  abstract protected function authenticate();
  
  /**
   * returns TRUE if $container exists, FALSE otherwise. return NULL on 
   * error
   * @param string $container the container to check 
   * @return boolean
   */
  abstract public function containerExists($container);
  
  /**
   * deletes $object. returns TRUE on success, FALSE on failure
   * @param string $container the object container
   * @param string $object name of the object to delete
   * @return boolean
   */
  abstract public function deleteObject($container, $object);
  
  /**
   * returns the size of $object in bytes. return NULL on error
   * @param string $container the object container
   * @param string $object name of the object
   * @return int
   */
  abstract public function getObjectSize($container, $object);
  
  /**
   * returns an array corresponding with the names of the objects in $container
   * @param string $container the container to check 
   * @param string $prefix optional directory style prefix to limit results
   * (e.g. '/images/gifs')
   * @return array
   */
  abstract protected function listContainer($container, $prefix=NULL);
  
  /**
   * returns TRUE if the object identified by $name exists in $container. 
   * return NULL on error
   * @param string $container the container to check
   * @param string $object the name of the object
   * @return boolean
   */
  abstract public function objectExists($container, $object);
  
}
?>
