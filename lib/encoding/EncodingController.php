<?php
require_once(dirname(dirname(__FILE__)) . '/EncodingUtil.php');
require_once(dirname(dirname(__FILE__)) . '/storage/ObjectStorageController.php');
/**
 * Abstract class defining and providing partial implementation of an object 
 * storage controller. This class is used to interact with a supported object
 * storage system to facilitate encode testing
 */
abstract class EncodingController {
  
  const DEFAULT_AUDIO_AAC_PROFILE = 'auto';
  const DEFAULT_AUDIO_SAMPLE_RATE = 'auto';
  const DEFAULT_BFRAMES = 2;
  const DEFAULT_CONCURRENT_REQUESTS = 8;
  const DEFAULT_FORMAT = '_default_';
  const DEFAULT_FORMAT_AUDIO = 'aac';
  const DEFAULT_FORMAT_VIDEO = 'mp4';
  const DEFAULT_KEYFRAME = 250;
  const DEFAULT_REFERENCE_FRAMES = 3;
  const MAX_AUDIO_BITRATE = 1024;
  const MAX_BFRAMES = 16;
  const MAX_CONCURRENT_REQUESTS = 32;
  const MAX_HLS_SEGMENT = 1000;
  const MAX_KEYFRAME = 1000;
  const MAX_POLL_RETRIES = 3;
  const MAX_REFERENCE_FRAMES = 16;
  const MAX_VIDEO_BITRATE = 1048576;
  const CLEANUP_FILE = '.output_objects';
  // rounding precision for result metrics
  const ROUND_PRECISION = 4;
  // number of seconds to sleep before setting output sizes
  const SLEEP_BEFORE_SET_SIZE = 10;
  const STATUS_CODES = 'download,queue,encode,upload,success,fail,partial';
  const SUPPORTED_AUDIO_AAC_PROFILES = 'auto,aac-lc,he-aac,he-aacv2';
  const SUPPORTED_AUDIO_SAMPLE_RATES = 'auto,22050,32000,44100,48000,96000';
  const SUPPORTED_FORMATS = 'aac,mp3,mp4,ogg,webm,_default_';
  const SUPPORTED_PROFILES = 'baseline,main,high';
  const VALID_JOB_STATS = 'audio_aac_profile,audio_bit_rate,audio_channels,audio_codec,audio_sample_rate,duration,error,job_start,job_stop,job_time,output_audio_aac_profile,output_audio_bit_rate,output_audio_channels,output_audio_codecs,output_audio_sample_rates,output_durations,output_failed,output_formats,output_success,output_total_bit_rates,output_video_bit_rates,output_video_codecs,output_video_frame_rates,output_video_resolutions,total_bit_rate,video_bit_rate,video_codec,video_frame_rate,video_resolution';
  
  /**
   * Runtime properties - set automatically following instantiation. 
   * Documentation for each is provided in the README
   */
  protected $service_key;
  protected $service_region;
  protected $service_secret;
  
  // private attributes - may not be accessed by API implementations
  private $audio_aac_profile;
  private $audio_bitrate;
  private $audio_sample_rate;
  private $audio_codecs;
  private $bframes;
  private $cleanup;
  private $concurrent_requests;
  private $format;
  private $formats;
  private $frame_rate;
  private $hls;
  private $hls_segment;
  private $input;
  private $input_downloaders;
  private $input_min_segment;
  private $input_objects;
  private $input_formats;
  private $input_sizes;
  private $jobs;
  private $profile;
  private $reference_frames;
  private $service;
  private $storage_container;
  private $storage_controller;
  private $two_pass;
  private $width;
  private $video_bitrate;
  private $video_codecs;
  
  /**
   * deletes objects created during testing
   * @return boolean
   */
  public final function cleanup() {
    $success = TRUE;
    $cleanup_file = self::getCleanupFile();
    if (!$this->cleanup) EncodingUtil::log(sprintf('Skipping cleanup because cleanup parameter was set to 0'), 'EncodingController::cleanup', __LINE__);
    else if (file_exists($cleanup_file) && ($fp = fopen($cleanup_file, 'r'))) {
      EncodingUtil::log(sprintf('Starting cleanup using cleanup file %s', $cleanup_file), 'EncodingController::cleanup', __LINE__);
      $deleted = 0;
      while($object = fgets($fp)) {
        if (!($object = trim($object))) continue;
        if ($this->storage_controller->deleteObject($this->storage_controller->getContainer(), $object)) {
          EncodingUtil::log(sprintf('Deleted object %s/%s successfully', $this->storage_controller->getContainer(), $object), 'EncodingController::cleanup', __LINE__);
          $deleted++;
        }
        else {
          EncodingUtil::log(sprintf('Unable to delete object %s/%s', $this->storage_controller->getContainer(), $object), 'EncodingController::cleanup', __LINE__, TRUE);
          $success = FALSE;
        }
      }
      fclose($fp);
      if ($deleted) EncodingUtil::log(sprintf('Cleanup complete - %d objects deleted', $deleted), 'EncodingController::cleanup', __LINE__);
    }
    else {
      EncodingUtil::log(sprintf('Unable to initiate cleanup because object tracker file %s does not exist or controller has not been properly initialized', $cleanup_file), 'EncodingController::cleanup', __LINE__, TRUE);
      $success = FALSE;
    }
    
    // service cleanup
    if (!$this->cleanupService()) {
      $success = FALSE;
      EncodingUtil::log(sprintf('Service cleanup failed'), 'EncodingController::cleanup', __LINE__, TRUE);
    }
    
    return $success;
  }
  
  /**
   * returns the path to the file used to record objects created by test jobs
   * @return string
   */
  private static final function getCleanupFile() {
    return sprintf('%s/%s', getenv('bm_run_dir'), self::CLEANUP_FILE);
  }
  
  /**
   * Singleton method to use in order to instantiate
   * @return EncodingController
   */
  public static final function &getInstance() {
    // set some global settings/variables
    global $base_error_level;
    global $encoding_concurrent_requests;
    if (!ini_get('date.timezone')) ini_set('date.timezone', ($tz = trim(shell_exec('date +%Z'))) ? $tz : 'UTC');
    if (!isset($base_error_level)) $base_error_level = error_reporting();
    
    static $_instances;
    $service = getenv('bm_param_service');
    if (!isset($_instances[$service])) {
      $dir = dirname(__FILE__) . '/' . $service;
      if ($service && is_dir($dir)) {
        $d = dir($dir);
        $controller_file = NULL;
        while($file = $d->read()) {
          if (preg_match('/EncodingController\.php$/', $file)) $controller_file = $file;
        }
        if ($controller_file) {
          require_once($dir . '/' . $controller_file);
          $controller_class = str_replace('.php', '', basename($controller_file));
          if (class_exists($controller_class)) {
            EncodingUtil::log(sprintf('Instantiating new EncodingController using class %s', $controller_class), 'EncodingController::getInstance', __LINE__);
            $_instances[$service] = new $controller_class();
            if (is_subclass_of($_instances[$service], 'EncodingController')) {
              // set runtime parameters
              $_instances[$service]->service = $service;
              $_instances[$service]->audio_aac_profile = getenv('bm_param_audio_aac_profile') ? trim(strtolower(getenv('bm_param_audio_aac_profile'))) : self::DEFAULT_AUDIO_AAC_PROFILE;
              $_instances[$service]->audio_bitrate = getenv('bm_param_audio_bitrate') ? getenv('bm_param_audio_bitrate')*1 : NULL;
              $_instances[$service]->audio_sample_rate = getenv('bm_param_audio_sample_rate') ? trim(strtolower(getenv('bm_param_audio_sample_rate'))) : self::DEFAULT_AUDIO_SAMPLE_RATE;
              if ($_instances[$service]->audio_sample_rate != 'auto') $_instances[$service]->audio_sample_rate *= 1;
              if (getenv('bm_param_bframes') !== NULL) $_instances[$service]->bframes = getenv('bm_param_bframes')*1;
              if (!isset($_instances[$service]->bframes)) $_instances[$service]->bframes = self::DEFAULT_BFRAMES;
              $_instances[$service]->cleanup = getenv('bm_param_cleanup') === NULL || getenv('bm_param_cleanup') == '1';
              $_instances[$service]->concurrent_requests = getenv('bm_param_concurrent_requests')*1;
              if (!$_instances[$service]->concurrent_requests) $_instances[$service]->concurrent_requests = self::DEFAULT_CONCURRENT_REQUESTS;
              $encoding_concurrent_requests = $_instances[$service]->concurrent_requests;
              if (getenv('bm_param_reference_frames') !== NULL) $_instances[$service]->reference_frames = getenv('bm_param_reference_frames')*1;
              if (!isset($_instances[$service]->reference_frames)) $_instances[$service]->reference_frames = self::DEFAULT_REFERENCE_FRAMES;
              $_instances[$service]->format = trim(strtolower(getenv('bm_param_format')));
              $_instances[$service]->frame_rate = getenv('bm_param_frame_rate')*1;
              $_instances[$service]->hls = getenv('bm_param_hls');
              if ($_instances[$service]->hls) $_instances[$service]->hls *= 1;
              $_instances[$service]->hls_segment = getenv('bm_param_hls_segment')*1;
              if (!$_instances[$service]->hls_segment) $_instances[$service]->hls_segment = self::DEFAULT_HLS_SEGMENT;
              $_instances[$service]->input_downloaders = getenv('bm_param_input_downloaders')*1;
              if (getenv('bm_param_input_min_segment')) $_instances[$service]->input_min_segment = self::sizeToBytes(getenv('bm_param_input_min_segment'));
              $_instances[$service]->keyframe = getenv('bm_param_keyframe')*1;
              if (!$_instances[$service]->keyframe) $_instances[$service]->keyframe = self::DEFAULT_KEYFRAME;
              $_instances[$service]->profile = trim(strtolower(getenv('bm_param_profile')));
              $_instances[$service]->service_key = getenv('bm_param_service_key');
              $_instances[$service]->service_region = getenv('bm_param_service_region');
              $_instances[$service]->service_secret = getenv('bm_param_service_secret');
              $_instances[$service]->two_pass = !$_instances[$service]->hls && getenv('bm_param_two_pass') == '1';
              $_instances[$service]->width = getenv('bm_param_width')*1;
              $_instances[$service]->video_bitrate = getenv('bm_param_video_bitrate') ? getenv('bm_param_video_bitrate')*1 : NULL;
              if ($_instances[$service]->storage_controller =& ObjectStorageController::getInstance()) {
                if ($_instances[$service]->input = getenv('bm_param_input')) $_instances[$service]->input_objects = $_instances[$service]->storage_controller->getContainerObjects($_instances[$service]->input);
                // set input formats, input sizes and output formats
                if ($_instances[$service]->input_objects) {
                  $_instances[$service]->audio_codecs = array();
                  $_instances[$service]->video_codecs = array();
                  $_instances[$service]->formats = array();
                  $_instances[$service]->input_formats = array();
                  $_instances[$service]->input_sizes = array();
                  foreach($_instances[$service]->input_objects as $i => $object) {
                    $_instances[$service]->formats[$i] = $_instances[$service]->format == self::DEFAULT_FORMAT ? (EncodingUtil::isAudio($object) ? self::DEFAULT_FORMAT_AUDIO : self::DEFAULT_FORMAT_VIDEO) : $_instances[$service]->format;
                    // set codecs
                    switch($_instances[$service]->formats[$i]) {
                      case 'aac':
                        $_instances[$service]->audio_codecs[$i] = 'aac';
                        $_instances[$service]->video_codecs[$i] = NULL;
                        break;
                      case 'mp3':
                        $_instances[$service]->audio_codecs[$i] = 'mp3';
                        $_instances[$service]->video_codecs[$i] = NULL;
                        break;
                      case 'mp4':
                        $_instances[$service]->audio_codecs[$i] = 'aac';
                        $_instances[$service]->video_codecs[$i] = 'h264';
                        break;
                      case 'ogg':
                        $_instances[$service]->audio_codecs[$i] = 'vorbis';
                        $_instances[$service]->video_codecs[$i] = 'theora';
                        break;
                      case 'webm':
                        $_instances[$service]->audio_codecs[$i] = 'vorbis';
                        $_instances[$service]->video_codecs[$i] = 'vp8';
                        break;
                    }
                    $pieces = explode('.', $object);
                    $_instances[$service]->input_formats[$i] = trim(strtolower($pieces[count($pieces) - 1]));
                    if (!($_instances[$service]->input_sizes[$i] = $_instances[$service]->storage_controller->getSize($object))) {
                      EncodingUtil::log(sprintf('Unable to determine size of object %s', $object), 'EncodingController::getInstance', __LINE__, TRUE);
                      $_instances[$service] = NULL;
                      break;
                    }
                    
                    EncodingUtil::log(sprintf('Added object %s to encoding job queue. Format: %s; Encode Format: %s; Size: %d MB', $object, $_instances[$service]->input_formats[$i], $_instances[$service]->formats[$i], round(($_instances[$service]->input_sizes[$i]/1024)/1024, 4)), 'EncodingController::getInstance', __LINE__);
                  }
                }
                if ($_instances[$service]) {
                  EncodingUtil::log(sprintf('EncodingController implementation %s for service %s instantiated successfully. A total of %d MB from %d media files will be encoded. Initiating...', $controller_class, $service, round((array_sum($_instances[$service]->input_sizes)/1024)/1024, 4), count($_instances[$service]->input_objects)), 'EncodingController::getInstance', __LINE__);
                  if (!$_instances[$service]->init()) {
                    EncodingUtil::log(sprintf('Unable to initiate service - aborting test'), 'EncodingController::getInstance', __LINE__, TRUE);
                    $_instances[$service] = NULL;              
                  }
                  else if ($_instances[$service]->validate()) EncodingUtil::log(sprintf('Runtime validation successful for service %s', $service), 'EncodingController::getInstance', __LINE__);
                  else {
                    EncodingUtil::log(sprintf('Runtime parameters are invalid - aborting test'), 'EncodingController::getInstance', __LINE__, TRUE);
                    $_instances[$service] = NULL;
                  } 
                }
              }
              else {
                EncodingUtil::log(sprintf('Unable to initiate storage service'), 'EncodingController::getInstance', __LINE__, TRUE);
                $_instances[$service] = NULL;
              }
            }
            else EncodingUtil::log(sprintf('EncodingController implementation %s for service %s does not extend the base class EncodingController', $controller_class, $service), 'EncodingController::getInstance', __LINE__, TRUE);
          }
        }
        else EncodingUtil::log(sprintf('EncodingController implementation not found for service %s', $service), 'EncodingController::getInstance', __LINE__, TRUE);
      }
      else if ($service) EncodingUtil::log(sprintf('service parameter "%s" is not valid', $service), 'EncodingController::getInstance', __LINE__, TRUE); 
      else EncodingUtil::log('service parameter is not set', 'EncodingController::getInstance', __LINE__, TRUE); 
    }
    
    if (isset($_instances[$service])) return $_instances[$service];
    else return $nl = NULL;
  }
  
  /**
   * returns the h.264 profile level to use for $profile
   * @param string $profile the profile: baseline, main or high
   * @return float
   */
  protected final function getH264ProfileLevel($profile) {
    if (!$profile) $profile = 'baseline';
    $level = NULL;
    switch($profile) {
      case 'baseline':
        $level = 3;
        break;
      case 'main':
        $level = 3.1;
        break;
      case 'high':
        $level = 4;
        break;
    }
    EncodingUtil::log(sprintf('Returning profile level %s for profile %s', $level, $profile), 'EncodingController::getH264ProfileLevel', __LINE__);
    
    return $level;
  }
  
  /**
   * Returns the number of downloaders to use for an input of size $bytes. This
   * is derived from the input_downloaders and input_min_segment runtime 
   * parameters
   * @param int $bytes size of the input file in bytes
   * @return int
   */
  protected final function getNumberOfDownloaders($bytes) {
    $downloaders = 1;
    
    if ($this->input_downloaders > 1) {
      EncodingUtil::log(sprintf('Calculating number of downloaders for %s MB input file using input_downloaders %d and input_min_segment %s MB', round(($bytes/1024)/1024, 4), $this->input_downloaders, round(($this->input_min_segment/1024)/1024, 4)), 'EncodingController::getNumberOfDownloaders', __LINE__);
      $downloaders = $this->input_downloaders;
      $segment_size = $bytes/$downloaders;
      if ($this->input_min_segment && $segment_size < $this->input_min_segment) {
        $downloaders = floor($bytes/$this->input_min_segment);
        if (!$downloaders) $downloaders = 1;
        EncodingUtil::log(sprintf('Using %d downloaders because segment size %s MB with %d downloaders is smaller than input_min_segment %s', $downloaders, round(($segment_size/1024)/1024, 4), $this->input_downloaders, round(($this->input_min_segment/1024)/1024, 4)), 'EncodingController::getNumberOfDownloaders', __LINE__);
      }
      else EncodingUtil::log(sprintf('Using default %d downloaders with segment size %s MB', $downloaders, round(($segment_size/1024)/1024, 4)), 'EncodingController::getNumberOfDownloaders', __LINE__);
    }
    else EncodingUtil::log(sprintf('input_downloaders setting not specified or set to 1 - download concurrency will not be used'), 'EncodingController::getNumberOfDownloaders', __LINE__);
    
    return $downloaders;
  }
  
  /**
   * Returns the name of the encoding service
   * @return string
   */
  public final function getService() {
    return $this->service;
  }
  
  /**
   * Polls for completion of test encoding jobs. returns TRUE if all jobs are
   * complete, FALSE otherwise
   * @param int $retry used to initiate re-tries (up to MAX_POLL_RETRIES)
   * @return boolean
   */
  public final function poll($retry=0) {
    $complete = TRUE;
    if (isset($this->jobs) && count($this->jobs)) {
      $pollJobs = array();
      foreach(array_keys($this->jobs) as $jobId) if (!isset($this->jobs[$jobId]['log']['success']) && !isset($this->jobs[$jobId]['log']['fail']) && !isset($this->jobs[$jobId]['log']['partial'])) $pollJobs[] = $jobId;
      
      EncodingUtil::log(sprintf('Checking on status of jobs %s', implode(', ', $pollJobs)), 'EncodingController::poll', __LINE__);
      if ($status = $this->getJobStatus($pollJobs)) {
        $status_codes = explode(',', self::STATUS_CODES);
        foreach($pollJobs as $jobId) {
          if (isset($status[$jobId])) {
            if (in_array($status[$jobId], $status_codes)) {
              // status has not changed (also cannot revert status)
              if ($this->jobs[$jobId]['status'] == $status[$jobId] || isset($this->jobs[$jobId]['log'][$status[$jobId]])) {
                EncodingUtil::log(sprintf('Status of job %s has not changed from %s', $jobId, $status[$jobId]), 'EncodingController::poll', __LINE__);
              }
              // status changed
              else {
                $ostatus = $this->jobs[$jobId]['status'];
                $this->jobs[$jobId]['times'][$ostatus] = microtime(TRUE) - $this->jobs[$jobId]['log'][$ostatus];
                $this->jobs[$jobId]['log'][$status[$jobId]] = microtime(TRUE);
                $this->jobs[$jobId]['status'] = $status[$jobId];
                if ($status[$jobId] == 'fail' || $status[$jobId] == 'partial' || $status[$jobId] == 'success') $this->jobs[$jobId]['stop'] = microtime(TRUE);
                EncodingUtil::log(sprintf('Status of job %s changed from %s to %s in %s secs', $jobId, $ostatus, $status[$jobId], $this->jobs[$jobId]['times'][$ostatus]), 'EncodingController::poll', __LINE__);
              }
            }
            else {
              EncodingUtil::log(sprintf('Encoding service provided invalid status %s for job ID %s. Setting job to fail', $status[$jobId], $jobId), 'EncodingController::poll', __LINE__, TRUE);
              $this->jobs[$jobId]['log']['fail'] = microtime(TRUE);
              $this->jobs[$jobId]['stop'] = microtime(TRUE);
            }
          }
          else {
            EncodingUtil::log(sprintf('Encoding service failed to return status for job ID %s. Setting job to fail', $jobId), 'EncodingController::poll', __LINE__, TRUE);
            $this->jobs[$jobId]['log']['fail'] = microtime(TRUE);
            $this->jobs[$jobId]['stop'] = microtime(TRUE);
          }
        }
        $incomplete = array();
        foreach(array_keys($this->jobs) as $jobId) if (!isset($this->jobs[$jobId]['log']['success']) && !isset($this->jobs[$jobId]['log']['fail']) && !isset($this->jobs[$jobId]['log']['partial'])) $incomplete[] = $jobId;
        $complete = count($incomplete) ? FALSE : TRUE;
      }
      else if ($retry < self::MAX_POLL_RETRIES) {
        EncodingUtil::log(sprintf('Failed to get status from encoding service - initiating retry attempt %d', $retry + 1), 'EncodingController::poll', __LINE__);
        $complete = $this->poll($retry + 1);
      }
      else {
        EncodingUtil::log(sprintf('Failed to get status from encoding service. Test aborting'), 'EncodingController::poll', __LINE__, TRUE);
        foreach($pollJobs as $jobId) {
          $this->jobs[$jobId]['log']['fail'] = microtime(TRUE);
          $this->jobs[$jobId]['stop'] = microtime(TRUE);
        }
      }
    }
    return $complete;
  }
  
  /**
   * sets the output_size metric for successful jobs. this method also sets 
   * cleanup objects, the output_files count and job_stats
   * @return void
   */
  public final function setOutputSizes() {
    if (is_array($this->jobs) && count($this->jobs)) {
      EncodingUtil::log(sprintf('Setting output size - sleeping %d seconds before starting', self::SLEEP_BEFORE_SET_SIZE), 'EncodingController::setOutputSizes', __LINE__);
      sleep(self::SLEEP_BEFORE_SET_SIZE);
      $cleanup_file = self::getCleanupFile();
      $fp = fopen($cleanup_file, 'w');
      foreach($this->jobs as $jobId => $job) {
        $this->jobs[$jobId]['job_stats'] = $this->jobStats($jobId);
        EncodingUtil::log(sprintf('Calculating output size for job %s using prefix %s/*', $jobId, $job['output_prefix']), 'EncodingController::setOutputSizes', __LINE__);
        $this->jobs[$jobId]['output_size'] = 0;
        if ($outputs = $this->storage_controller->getContainerObjects($job['output_prefix'] . '/*')) {
          EncodingUtil::log(sprintf('%d output objects exist for job %s', count($outputs), $jobId), 'EncodingController::setOutputSizes', __LINE__);
          $this->jobs[$jobId]['output_files'] = count($outputs);
          foreach($outputs as $output) {
            fwrite($fp, $output . "\n");
            EncodingUtil::log(sprintf('Getting size of output object %s for job %s', $output), 'EncodingController::setOutputSizes', __LINE__);
            if ($size = $this->storage_controller->getObjectSize($this->storage_controller->getContainer(), $output)) {
              EncodingUtil::log(sprintf('Got output size %s MB for output object %s and job %s', round(($size/1024)/1024, 4), $output, $jobId), 'EncodingController::setOutputSizes', __LINE__);
              $this->jobs[$jobId]['output_size'] += $size;
            }
            else EncodingUtil::log(sprintf('Unable to get output size for %s', $output), 'EncodingController::setOutputSizes', __LINE__, TRUE);
          }
          EncodingUtil::log(sprintf('Set total output size %s MB from %d output objects for job %s', round(($this->jobs[$jobId]['output_size']/1024)/1024, 4), count($outputs), $jobId), 'EncodingController::setOutputSizes', __LINE__);
        }
        else EncodingUtil::log(sprintf('No objects exist for job %s', $jobId), 'EncodingController::setOutputSizes', __LINE__, TRUE);
      }
      fclose($fp);
    }
  }
  
  /**
   * converts a size label like 5MB or 1000KB to its numeric byte equivalent. 
   * returns NULL if $size is not valid
   * @param string $size the size label to convert
   * @return int
   */
  public static final function sizeToBytes($size) {
    $bytes = NULL;
    if (is_numeric($size)) $bytes = $size*1;
    else if (preg_match('/^([0-9]+)\s*([gmk]?[b])$/i', trim(strtolower($size)), $m)) {
      $factor = 1;
      switch($m[2]) {
        case 'kb':
          $factor = 1024;
          break;
        case 'mb':
          $factor = 1024*1024;
          break;
        case 'gb':
          $factor = 1024*1024*1024;
          break;
      }
      $bytes = $m[1]*$factor;
    }
    return $bytes;
  }
  
  /**
   * Starts the desired encoding tests
   * @return boolean
   */
  public final function start() {
    // only call once
    if (isset($this->jobs)) return count($this->jobs) ? TRUE : FALSE;
    
    $this->jobs = array();
    EncodingUtil::log(sprintf('Starting tests'), 'EncodingController::run', __LINE__);
    $outputs = array();
    // define job settings
    if ($this->hls) {
      $hls_settings = parse_ini_file(dirname(__FILE__) . '/hls.ini', TRUE);
      foreach(array_keys($hls_settings) as $key) {
        if (preg_match('/^hls([0-9]+)$/', $key, $m)) {
          $bit = $m[1]*1;
          if ($bit & $this->hls) {
            if ($audio_bitrate = $hls_settings[$key]['audio_bitrate']) {
              $frame_rate = isset($hls_settings[$key]['frame_rate']) ? $hls_settings[$key]['frame_rate'] : NULL;
              $h264_profile = isset($hls_settings[$key]['profile']) ? $hls_settings[$key]['profile'] : NULL;
              $keyframe = isset($hls_settings[$key]['keyframe']) ? $hls_settings[$key]['keyframe'] : NULL;
              $video_bitrate = isset($hls_settings[$key]['video_bitrate']) ? $hls_settings[$key]['video_bitrate'] : NULL;
              $res = isset($hls_settings[$key]['res16:9']) ? $hls_settings[$key]['res16:9'] : NULL;
              if ($res) {
                $pieces = explode('x', $res);
                $width = $pieces[0];
              }
              else $width = NULL;
              EncodingUtil::log(sprintf('Adding hls key %s to output jobs with audio_bitrate: %d; h264_profile: %s; keyframe: %d; video_bitrate: %d; frame_rate: %s; width %d', $key, $audio_bitrate, $h264_profile, $keyframe, $video_bitrate, $frame_rate, $width), 'EncodingController::run', __LINE__);
              $output = array('audio_bitrate' => $audio_bitrate);
              
              if ($video_bitrate) {
                $output['video_bitrate'] = $video_bitrate;
                if ($frame_rate) $output['frame_rate'] = $frame_rate;
                if ($h264_profile) $output['h264_profile'] = $h264_profile;
                if ($keyframe) $output['keyframe'] = $keyframe;
                if ($width) $output['width'] = $width; 
              }
              else $output['audio_only'] = TRUE;
              $outputs[] = $output;
            }
            else EncodingUtil::log(sprintf('Skipping hls ini key %s because no audio_bitrate has been defined (required)', $key), 'EncodingController::run', __LINE__, TRUE);
          }
          else EncodingUtil::log(sprintf('Skipping hls key %s because %d is not in the hls setting parameter %d', $key, $bit, $this->hls), 'EncodingController::run', __LINE__);
        }
        else EncodingUtil::log(sprintf('Invalid hls ini key %s', $key), 'EncodingController::run', __LINE__, TRUE);
      }
    }
    else {
      $output = array();
      if ($this->audio_bitrate) $output['audio_bitrate'] = $this->audio_bitrate;
      if ($this->frame_rate) $output['frame_rate'] = $this->frame_rate;
      if ($this->profile) $output['h264_profile'] = $this->profile;
      if ($this->keyframe) $output['keyframe'] = $this->keyframe;
      if ($this->video_bitrate) $output['video_bitrate'] = $this->video_bitrate;
      if ($this->width) $output['width'] = $this->width;
      $outputs[] = $output;
    }
    foreach($this->input_objects as $i => $object) {
      $oformat = $this->formats[$i];
      $audio_only = EncodingUtil::isAudio($object) || EncodingUtil::isAudio($oformat);
      
      $iformat = $this->input_formats[$i];
      $audio_codec = $this->audio_codecs[$i];
      $video_codec = $audio_only ? NULL : $this->video_codecs[$i];
      $size = $this->input_sizes[$i];
      $size_mb = round(($size/1024)/1024, 4);
      $output_prefix = sprintf('ch%d', rand());
      
      EncodingUtil::log(sprintf('Initiating encoding for input object %s, input format %s, input size %s MB, output format %s; audio_codec %s; video_codec %s. %d output files will be encoded', $object, $iformat, $size_mb, $oformat, $audio_codec, $video_codec, count($outputs)), 'EncodingController::run', __LINE__);
      // set output files
      $job_outputs = array();
      $output_num = 0;
      foreach($outputs as $i => $output) {
        // audio only
        if (!isset($output['audio_only'])) $output['audio_only'] = $audio_only;
        // incompatible video output
        if ($output['audio_only'] && isset($output['video_bitrate'])) {
          // HLS - remove output
          if (count($outputs) > 1) {
            EncodingUtil::log('Removed video output for audio only job', 'EncodingController::run', __LINE__);
            continue;
          }
          // Non-HLS - remove video settings
          else {
            EncodingUtil::log('Removing video settings for audio only job', 'EncodingController::run', __LINE__);
            unset($output['video_bitrate']);
            if (isset($output['frame_rate'])) unset($output['frame_rate']);
            if (isset($output['h264_profile'])) unset($output['h264_profile']);
            if (isset($output['keyframe'])) unset($output['keyframe']);
            if (isset($output['width'])) unset($output['width']);
          }
        }
        $ofile = sprintf('%s/%s_a%s%s_%d.%s', $output_prefix, str_replace('.' . strtoupper($iformat), '', str_replace('.' . $iformat, '', basename($object))), isset($output['audio_bitrate']) ? $output['audio_bitrate'] : '-def', $output['audio_only'] ? '' : '_v' . (isset($output['video_bitrate']) ? $output['video_bitrate'] : '-def'), ++$output_num, $this->hls ? 'm3u8' : $oformat);
        $output['output'] = $ofile;
        $debug = 'Adding output file with settings: ';
        foreach($output as $key => $val) $debug .= $key . '=' . $val . '; ';
        EncodingUtil::log($debug, 'EncodingController::run', __LINE__);
        $job_outputs[] = $output;
      }
      
      if (!count($job_outputs)) EncodingUtil::log('Unable to start encoding job - there are no outputs', 'EncodingController::run', __LINE__, TRUE);
      else if ($jobId = $this->encode($this->storage_controller, $object, $iformat, $size, $oformat, $this->audio_aac_profile, $audio_codec, $this->audio_sample_rate, $video_codec, $this->bframes, $this->reference_frames, $this->two_pass, $this->hls ? TRUE : FALSE, $this->hls_segment, $job_outputs)) {
        EncodingUtil::log(sprintf('Encoding job started successfully - job ID %s', $jobId), 'EncodingController::run', __LINE__);
        $status = $this->initialStatusDownload() ? 'download' : 'queue';
        $log = array();
        $log[$status] = microtime(TRUE);
        $this->jobs[$jobId] = array('input' => $object, 'input_format' => $iformat, 'input_size' => $size, 'output_prefix' => $output_prefix, 'outputs' => $job_outputs, 'log' => $log, 'start' => microtime(TRUE), 'status' => $status, 'times' => array());
      }
      else EncodingUtil::log('Unable to start encoding job', 'EncodingController::run', __LINE__, TRUE);
    }
    return count($this->jobs) ? TRUE : FALSE;
  }
  
  /**
   * Prints the test stats
   * @return void
   */
  public final function stats() {
    if (is_array($this->jobs) && count($this->jobs)) {
      $valid_job_stats = explode(',', self::VALID_JOB_STATS);
      $same_region = $this->sameRegion($this->storage_controller);
      $i = 1;
      foreach($this->jobs as $jobId => $job) {
        $suffix = count($this->jobs) > 1 ? $i++ : '';
        printf("input%s=%s\n", $suffix, $job['input']);
        if (isset($job['input_format'])) printf("input_format%s=%s\n", $suffix, $job['input_format']);
        if (isset($job['input_size'])) {
          printf("input_size%s=%s\n", $suffix, $job['input_size']);
          printf("input_size_mb%s=%s\n", $suffix, round(($job['input_size']/1024)/1024, self::ROUND_PRECISION));
        }
        printf("job_id%s=%s\n", $suffix, $jobId);
        printf("job_status%s=%s\n", $suffix, $job['status']);
        if (is_array($job['job_stats'])) {
          ksort($job['job_stats']);
          foreach($job['job_stats'] as $stat => $val) {
            if (in_array($stat, $valid_job_stats)) {
              if ($stat == 'duration') $val = round($val, self::ROUND_PRECISION);
              printf("%s%s%s=%s\n", $stat == 'error' || preg_match('/^job_/', $stat) || preg_match('/^output_/', $stat) ? '' : 'input_', $stat, $suffix, $val);
            }
          }
        }
        if (isset($job['output_files']) && $job['output_files']) {
          printf("output_files%s=%d\n", $suffix, $job['output_files']);
          printf("output_size%s=%d\n", $suffix, $job['output_size']);
          $output_mb = round(($job['output_size']/1024)/1024, self::ROUND_PRECISION);
          printf("output_size_mb%s=%s\n", $suffix, $output_mb);
          printf("same_region%s=%s\n", $this->sameRegion($this->storage_controller));
          printf("size_ratio%s=%s\n", $suffix, round(($job['output_size']/$job['input_size'])*100, self::ROUND_PRECISION));
          // output minutes
          if (isset($job['job_stats']['output_durations'])) {
            $output_minutes = 0;
            foreach(explode(',', $job['job_stats']['output_durations']) as $duration) {
              if ($duration) $output_minutes += ($duration/60);
            }
            if ($output_minutes) {
              printf("output_mb_minute%s=%s\n", $suffix, round($output_mb/$output_minutes, self::ROUND_PRECISION));
              printf("output_minutes%s=%s\n", $suffix, round($output_minutes, self::ROUND_PRECISION));
            }
          }
        }
        printf("same_region%s=%d\n", $suffix, $same_region);
        if (isset($job['start'])) printf("start%s=%d\n", $suffix, $job['start']);
        if (isset($job['stop'])) printf("stop%s=%d\n", $suffix, $job['stop']);
        if (isset($job['start']) && isset($job['stop'])) printf("time%s=%s\n", $suffix, round($job['stop'] - $job['start'], self::ROUND_PRECISION));
        if (isset($job['times'])) {
          ksort($job['times']);
          foreach($job['times'] as $key => $time) printf("time_%s%s=%s\n", $key, $suffix, round($time, self::ROUND_PRECISION));
        }
      }
    }
  }
  
  /**
   * Validates runtime parameters - returns TRUE on success, FALSE on failure.
   * If a validation failure occurs, the relevant error message will be logged
   */
  public final function validate() {
    if (!isset($this->validated)) {
      $this->validated = TRUE;
      
      // audio aac profile
      if ($this->audio_aac_profile && !in_array($this->audio_aac_profile, explode(',', self::SUPPORTED_AUDIO_AAC_PROFILES))) {
        EncodingUtil::log(sprintf('Invalid audio_aac_profile %s', $this->audio_aac_profile), 'EncodingController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      
      // audio bitrate
      if ($this->audio_bitrate && ($this->audio_bitrate < 0 || $this->audio_bitrate > self::MAX_AUDIO_BITRATE)) {
        EncodingUtil::log(sprintf('Invalid audio_bitrate %d', $this->audio_bitrate), 'EncodingController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      
      // audio aac profile
      if ($this->audio_sample_rate && !in_array($this->audio_sample_rate, explode(',', self::SUPPORTED_AUDIO_SAMPLE_RATES))) {
        EncodingUtil::log(sprintf('Invalid audio_sample_rate %s', $this->audio_sample_rate), 'EncodingController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      
      // format
      if (!$this->format || !in_array($this->format, explode(',', self::SUPPORTED_FORMATS))) {
        EncodingUtil::log(sprintf('Invalid format %s', $this->format), 'EncodingController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      
      // input
      if ($this->input_objects === NULL) {
        EncodingUtil::log(sprintf('Error getting input objects %s', $this->input), 'EncodingController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      else if (!$this->input_objects) {
        EncodingUtil::log(sprintf('No matching input objects %s', $this->input), 'EncodingController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      else EncodingUtil::log(sprintf('Input objects parameter %s results in %d objects: %s', $this->input, count($this->input_objects), implode(', ', $this->input_objects)), 'EncodingController::validate', __LINE__);
      
      // bframes
      if ($this->bframes < 0 || $this->bframes > self::MAX_BFRAMES) {
        EncodingUtil::log(sprintf('Invalid bframes %d', $this->bframes), 'EncodingController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      
      // concurrent_requests
      if ($this->concurrent_requests < 1 || $this->concurrent_requests > self::MAX_CONCURRENT_REQUESTS) {
        EncodingUtil::log(sprintf('Invalid concurrent_requests %d', $this->concurrent_requests), 'EncodingController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      
      // reference_frames
      if ($this->reference_frames < 0 || $this->reference_frames > self::MAX_REFERENCE_FRAMES) {
        EncodingUtil::log(sprintf('Invalid reference_frames %d', $this->reference_frames), 'EncodingController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      
      // hls_segment
      if ($this->hls_segment < 0 || $this->hls_segment > self::MAX_HLS_SEGMENT) {
        EncodingUtil::log(sprintf('Invalid hls_segment %d', $this->hls_segment), 'EncodingController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      
      // keyframe
      if ($this->keyframe < 0 || $this->keyframe > self::MAX_KEYFRAME) {
        EncodingUtil::log(sprintf('Invalid keyframe %d', $this->keyframe), 'EncodingController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      
      // profile
      if ($this->profile && !in_array($this->profile, explode(',', self::SUPPORTED_PROFILES))) {
        EncodingUtil::log(sprintf('Invalid format %s', $this->format), 'EncodingController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      
      // encoding service region
      if (!$this->validateServiceRegion($this->service_region)) {
        EncodingUtil::log(sprintf('service_region %s is not valid', $this->service_region), 'EncodingController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      
      // video bitrate
      if ($this->video_bitrate && ($this->video_bitrate < 0 || $this->video_bitrate > self::MAX_VIDEO_BITRATE)) {
        EncodingUtil::log(sprintf('Invalid video_bitrate %d', $this->video_bitrate), 'EncodingController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      
      // encoding service credentials
      if (!$this->service_key) {
        EncodingUtil::log(sprintf('service_key is required'), 'EncodingController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      else if ($this->service_key && !$this->authenticate()) {
        EncodingUtil::log(sprintf('Encoding service authentication failed using key %s; region %s', $this->service_key, $this->service_region), 'EncodingController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      else EncodingUtil::log(sprintf('Encoding service authentication successful'), 'EncodingController::validate', __LINE__);
    }
    return $this->validated;
  }
  
  
  // these methods may by overriden by an API implementation
  
  /**
   * invoked following test completion. May be used to perform cleanup tasks.
   * Should return TRUE on success, FALSE on failure
   * @return boolean
   */
  protected function cleanupService() {
    return TRUE;
  }
  
  /**
   * return TRUE if the initial status of an encoding job is 'download', 
   * meaning the input must first be downloaded from the origin to the 
   * encoding service. If overridden and returns FALSE, it will be assumed that
   * the initial status is 'queue'
   * @return boolean
   */
  protected function initialStatusDownload() {
    return TRUE;
  }
  
  /**
   * may be used to perform pre-usage initialization. return TRUE on success, 
   * FALSE on failure
   * @return boolean
   */
  protected function init() {
    return TRUE;
  }
  
  /**
   * may be overridden to provide stats about a job input. These will be 
   * included in the test output if provided. The return value is a hash. The 
   * following stats may be returned:
   *   audio_aac_profile        input audio aac profile (aac-lc, he-aac, he-aacv2)
   *   audio_bit_rate           input audio bit rate (kbps)
   *   audio_channels           input audio channels
   *   audio_codec              input audio codec
   *   audio_sample_rate        Sample rate of input audio
   *   duration                 duration (seconds - decimal) of the media file
   *   error                    optional error message(s)
   *   job_start                start time for the job as reported by the service 
   *                            (optional)
   *   job_stop                 stop time for the job as reported by the service 
   *                            (optional)
   *   job_time                 the total time for the job as reported by the service
   *   output_audio_aac_profiles output audio aac profiles (csv) - reported by 
   *                            encoding service (optional) (aac-lc, he-aac, he-aacv2)
   *   output_audio_bit_rate    output audio bit rates (csv) - reported by encoding 
   *                            service (optional)
   *   output_audio_channels    output audio channels (csv) - reported by encoding 
   *                            service (optional)
   *   output_audio_codecs      output audio codecs (csv) - reported by encoding 
   *                            service (optional)
   *   output_audio_sample_rates Sample rates of output audio (csv) - reported by 
   *                            encoding service (optional)
   *   output_durations         Output durations (csv) - reported by encoding service 
   *                            (optional)
   *   output_failed            Number of outputs that failed to generate
   *   output_formats           Output formats (csv) - reported by encoding service 
   *                            (optional)
   *   output_success           Number of successful outputs generated
   *   output_total_bit_rates   Output total bit rates - kbps (csv) - as reported by 
   *                            encoding service (optional)
   *   output_video_bit_rates   Output video bit rates - kbps (csv) - as reported by 
   *                            encoding service (optional)
   *   output_video_codecs      Output video codecs (csv) - as reported by encoding 
   *                            service (optional)
   *   output_video_frame_rates Output video frame rates (csv) - as reported by 
   *                            encoding service (optional)
   *   output_video_resolutions Output video resolutions (csv) - reported by encoding 
   *                            service (optional)
   *   total_bit_rate           total bit rate of media file (kbps)
   *   video_bit_rate           input video bit rate
   *   video_codec              input video codec
   *   video_frame_rate         input video frame rate (kbps)
   *   video_resolution         input video resolution (WxH)
   * This method is only called for jobs that have successfully completed.
   * Return NULL on error
   * @param string $jobId the id of the job to return stats for
   * @return array
   */
  protected function jobStats($jobId) {
    return NULL;
  }
  
  /**
   * return TRUE if the service region $region is valid, FALSE otherwise
   * @param string $region the (encoding) region to validate (may be NULL if
   * use did not specify an explicit region)
   * @return boolean
   */
  protected function validateServiceRegion($region) {
    return TRUE;
  }
  
  
  // these methods must be defined for each API implementation
  
  /**
   * invoked once during validation. Should return TRUE if authentication is 
   * successful, FALSE otherwise. this method may reference the optional 
   * attributes $service_key, $service_secret and $service_region as necessary 
   * to complete authentication
   * @return boolean
   */
  abstract protected function authenticate();
  
  /**
   * Initiates an encoding job based on the parameters specified. Returns a 
   * job identifier on success, NULL on failure
   * @param ObjectStorageController $storage_controller the storage controller
   * implementation for input and output files
   * @param string $input input file name within the storage container
   * @param string $input_format input file format
   * @param int $input_size input file size (bytes)
   * @param string $format desired output format. One of: aac, mp3, mp4, ogg, webm
   * @param string $audio_aac_profile desired audio aac profile. One of: auto, 
   * aac-lc, he-aac, he-aacv2
   * @param string $audio_codec desired audio codec. One of: aac, mp3, vorbis
   * @param string $audio_sample_rate desired audio sample rate. One of: auto, 
   * 22050, 32000, 44100, 48000, 96000
   * @param string $video_codec desired video codec. One of: h264, theora, vp8
   * @param int $bframes max number of consecutive B-frames for h.264 output
   * @param int $reference_frames number of reference frames to use for h.264 
   * output
   * @param boolean $two_pass use 2 pass video encoding
   * @param boolean $hls outputs are part of an HLS playlist
   * @param int $hls_segment HLS segment (seconds)
   * @param array $outputs 1 or more output files to encode. An array of hashes
   * where each hash has the following possible keys:
   *   audio_bitrate  desired audio bitrate
   *   audio_only     TRUE for audio only output
   *   frame_rate     Optional specific frame rate to target
   *   h264_profile   h264 profile - one of: baseline - Baseline, 3.0; 
   *                  main - Main, 3.1; high - High
   *   keyframe       Number of keyframes per second
   *   output         path in $storage_container where the encoded file should 
   *                  be written to
   *   video_bitrate  Desired video bitrate
   *   width          Desired video resolution width (height should be adjusted 
   *                  accordingly)
   * @return string
   */
  abstract protected function encode(&$storage_controller, $input, $input_format, $input_size, $format, $audio_aac_profile, $audio_codec, $audio_sample_rate, $video_codec, $bframes, $reference_frames, $two_pass, $hls, $hls_segment, $outputs);
  
  /**
   * return a hash defining the current state of $jobIds. The hash should be
   * indexed by job ID where the value is the state of that job. Valid states
   * include:
   *   download  input file is downloading to encoding servers
   *   queue     job is queued for processing
   *   encode    job is processing
   *   upload    job has completed and outputs are uploading
   *   success   job completed successfully
   *   partial   job completed partially successful
   *   fail      job failed
   * @param array $jobIds the job IDs to return status for
   * @return NULL on error (test will abort)
   * @return array
   */
  abstract protected function getJobStatus($jobIds);
  
  /**
   * return TRUE if the designated storage service and region is the same as 
   * the encoding service
   * @param ObjectStorageController $storage_controller the storage controller
   * implementation for input and output files
   * @return boolean
   */
  abstract protected function sameRegion(&$storage_controller);
  
  
}
?>
