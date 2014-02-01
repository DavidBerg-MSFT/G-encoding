<?php
/**
 * Implements testing functionality for the Zencoder platform
 */
class EDCEncodingController extends EncodingController {
  
  // default service region
  const DEFAULT_ENCODING_COM_REGION = 'us-east-1';
  // base API URL
  const ENCODING_COM_API_URL = 'https://manage.encoding.com';
  // valid region identifiers
  const ENCODING_COM_REGIONS = 'us-east-1,us-west-1,us-west-2,eu-west-1,ap-southeast-1,ap-southeast-2,ap-northeast-1,sa-east-1';
  
  // encoding.com region
  private $edc_region;
  
  // multithreaded transport
  private $edc_multithread;
  
  // S3 nocopy
  private $edc_nocopy;
  
  // turbo
  private $edc_turbo;
  
  // twin turbo
  private $edc_twin_turbo;
  
  /**
   * converts an array into xml using a simple algorithm that produces valid
   * encoding.com xml
   * @param array $array the array to convert
   * @return string
   */
  private function arrayToXml($array) {
    $xml = '';
    foreach($array as $key => $val) {
      $xml .= sprintf('<%s>%s</%s>', $key, is_array($val) ? $this->arrayToXml($val) : str_replace('&', '&amp;', $val), $key);
    }
    return $xml;
  }
  
  /**
   * invoked once during validation. Should return TRUE if authentication is 
   * successful, FALSE otherwise. this method may reference the optional 
   * attributes $service_key, $service_secret and $service_region as necessary 
   * to complete authentication
   * @return boolean
   */
  protected function authenticate() {
    $authenticated = FALSE;
    if ($response = $this->invokeApi('GetMediaList')) {
      if ($authenticated = isset($response['response']) && is_array($response['response']) && !isset($response['response']['errors'])) EncodingUtil::log(sprintf('Authentication successful'), 'EDCEncodingController::authenticate', __LINE__);
      else EncodingUtil::log(sprintf('Authentication failed'), 'EDCEncodingController::authenticate', __LINE__, TRUE);
    }
    return $authenticated;
  }
  
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
  protected function encode(&$storage_controller, $input, $input_format, $input_size, $format, $audio_aac_profile, $audio_codec, $audio_sample_rate, $video_codec, $bframes, $reference_frames, $two_pass, $hls, $hls_segment, $outputs) {
    // set nocopy feature if it was not explicitely set
    if (!isset($this->edc_nocopy)) $this->edc_nocopy = $storage_controller->getApi() == 's3' && $this->sameRegion($storage_controller);
    
    $jobId = NULL;
    $job = array('userid' => 'xxx', 'userkey' => 'xxx', 'action' => NULL);
    $source = $storage_controller->getObjectUrl($input, TRUE);
    // add nocopy/multithread settings
    if ($this->edc_nocopy) $source .= '?nocopy';
    if ($this->edc_multithread) $source .= ($this->edc_nocopy ? '&' : '?') . 'multithread';
    $job['source'] = $source;
    $job['region'] = NULL;
    $job['format'] = array();
    // formats match up except for aac
    $format = $format == 'aac' ? 'm4a' : $format;
    
    // HLS
    if ($hls) {
      $job['format']['output'] = 'ipad_stream';
      $job['format']['pack_files'] = 'no';
      $job['format']['segment_duration'] = $hls_segment;
    }
    else $job['format']['output'] = $format;
    
    // audio_codec
    switch($audio_codec) {
      case 'vorbis':
        $audio_codec = 'libvorbis';
        break;
      case 'mp3':
        $audio_codec = 'libmp3lame';
        break;
      default:
        switch($audio_aac_profile) {
          case 'he-aac':
            $audio_codec = 'dolby_heaac';
            break;
          case 'he-aacv2':
            $audio_codec = 'dolby_heaacv2';
            break;
          default:
            $audio_codec = 'dolby_aac';
            break;
        }
        break;
    }
    $job['format']['audio_codec'] = $audio_codec;
    if ($audio_sample_rate != 'auto') $job['format']['audio_sample_rate'] = $audio_sample_rate;
    
    // video_codec
    $video_codec = $video_codec == 'h264' ? 'libx264' : ($video_codec == 'theora' ? 'libtheora' : 'libvpx');
    $job['format']['video_codec'] = $video_codec;
    
    // bframes
    $job['format']['bframes'] = $bframes > 1 ? 2 : 0;
    $job['format']['refs'] = $reference_frames;
    $job['format']['two_pass'] = $two_pass ? 'yes' : 'no';
    
    // turbo
    if ($this->edc_twin_turbo) $job['format']['twin_turbo'] = 'yes';
    else if ($this->edc_turbo) $job['format']['turbo'] = 'yes';
    
    // format settings
    foreach($outputs as $output) {
      // audio only - set the flag
      if ($hls && $output['audio_only']) $job['format']['add_audio_only'] = 'yes';
      else {
        if (isset($output['audio_bitrate'])) $job['format']['audio_bitrate'] = $output['audio_bitrate'] . 'k';
        if (!$output['audio_only']) {
          if (isset($output['width'])) $job['format']['size'] = (isset($job['format']['size']) ? $job['format']['size'] . ',' : '') . sprintf('%dx0', $output['width']);
          if (isset($output['video_bitrate'])) $job['format'][$key = $hls ? 'bitrates' : 'bitrate'] = (isset($job['format'][$key]) ? $job['format'][$key] . ',' : '') . $output['video_bitrate'] . 'k';
          if (isset($output['frame_rate'])) $job['format'][$key = $hls ? 'framerates' : 'framerate'] = (isset($job['format'][$key]) ? $job['format'][$key] . ',' : '') . $output['frame_rate'];
          if (isset($output['keyframe'])) $job['format'][$key = $hls ? 'keyframes' : 'keyframe'] = (isset($job['format'][$key]) ? $job['format'][$key] . ',' : '') . $output['keyframe'];
          // only 1 h.264 profile is supported - but 'baseline' is not allowed for some reason (it is the default profile)
          if ($format == 'mp4' && isset($output['h264_profile']) && $output['h264_profile'] != 'baseline') $job['format']['profile'] = $output['h264_profile']; 
        }
      }
      // destination - only 1 supported - need base name minus file extension
      $pieces = explode('_a', $output['output']);
      $job['format']['destination'] = $storage_controller->getObjectUrl($hls ? $pieces[0] : $output['output'], TRUE);
    }
    $debug = '';
    foreach($job['format'] as $k => $v) $debug .= ($debug ? '; ' : '') . sprintf('%s=%s', $k, $v);
    EncodingUtil::log(sprintf('Initiating encoding using source %s and settings: %s', $source, $debug), 'EDCEncodingController::encode', __LINE__);
    
    if ($response = $this->invokeApi('AddMedia', $job, TRUE)) {
      if ($jobId = isset($response['response']['MediaID']) ? $response['response']['MediaID'] : NULL) EncodingUtil::log(sprintf('Initiated encoding for %s successfully. MediaID %s', $input, $jobId), 'EDCEncodingController::encode', __LINE_);
      else EncodingUtil::log(sprintf('Unable to initiate encoding for %s - API request successful, but MediaID not included in response', $input), 'EDCEncodingController::encode', __LINE__, TRUE);
    }
    else EncodingUtil::log(sprintf('Unable to initiate encoding for %s', $input), 'EDCEncodingController::encode', __LINE__, TRUE);
    
    return $jobId;
  }
  
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
  protected function getJobStatus($jobIds) {
    $status = array();
    if (is_array($jobIds) && count($jobIds)) {
      if ($response = $this->invokeApi('GetStatus', array('extended' => 'yes', 'mediaid' => implode(',', $jobIds)))) {
        if (isset($response['response']['job']) && is_array($response['response']['job'])) {
          // convert single job response to array
          if (isset($response['response']['job']['id'])) $response['response']['job'] = array($response['response']['job']);
          foreach($response['response']['job'] as $job) {
            $jobId = $job['id'];
            $job_status = NULL;
            switch(trim(strtolower($job['status']))) {
              case 'new':
              case 'downloading':
                $job_status = 'download';
                break;
              case 'ready to process':
              case 'waiting for encoder':
                $job_status = 'queue';
                break;
              case 'processing':
                $job_status = 'encode';
                break;
              case 'saving':
                $job_status = 'upload';
                break;
              case 'finished':
                $job_status = 'success';
                break;
              case 'error':
                $job_status = 'fail';
                break;
            }
            // check for partial status
            if ($job_status == 'success' || $job_status == 'fail') {
              $error = 0;
              $finished = 0;
              if (isset($job['format']['status'])) $job['format'] = array($job['format']);
              foreach($job['format'] as $format) {
                trim(strtolower($format['status'])) == 'error' ? $error++ : $finished++;
              }
              if ($error && $finished) $job_status = 'partial';
              else if ($error) $job_status = 'fail';
              else $job_status = 'success';
            }
            if ($job_status) {
              $status[$jobId] = $job_status;
              EncodingUtil::log(sprintf('Returning status %s for job %s from status string %s', $job_status, $jobId, $job['status']), 'EDCEncodingController::getJobStatus', __LINE__);
            }
            else EncodingUtil::log(sprintf('Unable to determine status for job %s from status string %s', $jobId, $job['status']), 'EDCEncodingController::getJobStatus', __LINE__, TRUE);
          }
        }
        else EncodingUtil::log(sprintf('Unable to invoke GetStatus API - request successful, but "job" not included in response', $input), 'EDCEncodingController::getJobStatus', __LINE__, TRUE);
      }
      else EncodingUtil::log(sprintf('Unable to invoke GetStatus API query'), 'EDCEncodingController::getJobStatus', __LINE__, TRUE);
    }
    else EncodingUtil::log(sprintf('Invoked without specifying any jobIds'), 'EDCEncodingController::getJobStatus', __LINE__, TRUE);
    
    return count($status) ? $status : NULL;
  }
  
  /**
   * may be used to perform pre-usage initialization. return TRUE on success, 
   * FALSE on failure
   * @return boolean
   */
  protected function init() {
    $this->edc_multithread = getenv('bm_param_service_param1') === NULL || getenv('bm_param_service_param1') == '1';
    if (getenv('bm_param_service_param2') !== NULL) $this->edc_nocopy = getenv('bm_param_service_param2') == '1';
    $this->edc_region = $this->service_region ? $this->service_region : self::DEFAULT_ENCODING_COM_REGION;
    $this->edc_turbo = getenv('bm_param_service_param3') == 'turbo';
    $this->edc_twin_turbo = getenv('bm_param_service_param3') == 'twin_turbo';
    return TRUE;
  }
  
  /**
   * invokes an API action and returns the response. Automatically constructs 
   * the JSON request using the $action and credentials specified. Returns 
   * FALSE if the call results in a non 2xx status code. Returns an array on  
   * success. Returns NULL on error
   * @param string $action the action to invoke
   * @param array $attrs optional hash containing additional values to append
   * to the request
   * @param boolean $xml invoke API using XML formatted data instead of JSON
   * @return mixed
   */
  private function invokeApi($action, $attrs=NULL, $xml=FALSE) {
    $response = NULL;
    $attrs = is_array($attrs) ? $attrs : array();
    $attrs['action'] = $action;
    $attrs['userid'] = $this->service_key;
    $attrs['userkey'] = $this->service_secret;
    if ($this->edc_region != self::DEFAULT_ENCODING_COM_REGION) $attrs['region'] = $this->edc_region;
    $post = array('query' => $attrs);
    $body = $xml ? '<?xml version="1.0"?>' . $this->arrayToXml($post) : json_encode($post);
    $field = $xml ? 'xml' : 'json';
    $form = array();
    $form[$field] = $body;
    EncodingUtil::log(sprintf('Invoking API action %s using %s=%s', $action, $field, $body), 'EDCEncodingController::invokeApi', __LINE__);
    $request = array('method' => 'POST', 'url' => self::ENCODING_COM_API_URL, 'form' => $form);
    if ($result = EncodingUtil::curl(array($request), TRUE)) {
      if ($result['status'][0] >= 200 && $result['status'][0] < 300) {
        EncodingUtil::log(sprintf('API action %s completed successfully with status code %d and response: %s', $action, $result['status'][0], $result['body'][0]), 'EDCEncodingController::invokeApi', __LINE__);
        if (isset($result['body'][0]) && ($response = $result['body'][0])) {
          $response = $xml ? array('response' => $this->xmlToArray($response)) : json_decode($response, TRUE);
          EncodingUtil::log(sprintf('Returning API response: %s', json_encode($response)), 'EDCEncodingController::invokeApi', __LINE__);
        }
        else {
          EncodingUtil::log(sprintf('Unable to decode API response for action %s: %s', $action, $result['body'][0]), 'EDCEncodingController::invokeApi', __LINE__, TRUE);
          $response = NULL;
        }
      }
      else if ($result['status'][0] == 421) {
        EncodingUtil::log(sprintf('Got API rate throttling response code %d. Sleeping 1 second and retrying request', $result['status'][0]), 'EDCEncodingController::invokeApi', __LINE__, TRUE);
        sleep(1);
        return $this->invokeApi($action, $attrs, $xml);
      }
      else {
        EncodingUtil::log(sprintf('API action %s resulted in status code %d', $action, $result['status'][0]), 'EDCEncodingController::invokeApi', __LINE__, TRUE);
        $response = FALSE;
      }
    }
    else EncodingUtil::log(sprintf('API action %s could not be invoked', $action), 'EDCEncodingController::invokeApi', __LINE__, TRUE);
    return $response;    
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
    $stats = array();
    if ($jobId) {
      if ($response = $this->invokeApi('GetStatus', array('extended' => 'yes', 'mediaid' => $jobId))) {
        if (isset($response['response']['job']['id']) && $response['response']['job']['id'] == $jobId) {
          EncodingUtil::log(sprintf('Got status for job %s successfully - getting media info', $jobId), 'EDCEncodingController::jobStats', __LINE__);
          $job = $response['response']['job'];
          if ($response = $this->invokeApi('GetMediaInfo', array('mediaid' => $jobId))) {
            if (isset($response['response']['bitrate'])) {
              $media = $response['response'];
              if (isset($media['audio_bitrate'])) $stats['audio_bit_rate'] = $media['audio_bitrate']*1;
              if (isset($media['audio_channels'])) $stats['audio_channels'] = $media['audio_channels']*1;
              if (isset($media['audio_codec'])) $stats['audio_codec'] = $media['audio_codec'];
              if (isset($media['audio_sample_rate'])) $stats['audio_sample_rate'] = $media['audio_sample_rate']*1;
              if (isset($media['duration'])) $stats['duration'] = $media['duration']*1;
              if (isset($job['created'])) $stats['job_start'] = $job['created'];
              if (isset($job['finished'])) $stats['job_stop'] = $job['finished'];
              if (isset($stats['job_start']) && isset($stats['job_stop'])) $stats['job_time'] = strtotime($stats['job_stop']) - strtotime($stats['job_start']);
              if (isset($media['bitrate'])) $stats['total_bit_rate'] = $media['bitrate']*1;
              if (isset($media['video_bitrate'])) $stats['video_bit_rate'] = $media['video_bitrate']*1;
              if (!isset($stats['audio_bit_rate']) && isset($stats['total_bit_rate']) && isset($stats['video_bit_rate']) && $stats['total_bit_rate'] > $stats['video_bit_rate']) $stats['audio_bit_rate'] = $stats['total_bit_rate'] - $stats['video_bit_rate'];
              if (isset($media['video_codec'])) {
                $pieces = explode(' ', $media['video_codec']);
                $stats['video_codec'] = $pieces[0];
              }
              if (isset($media['frame_rate'])) $stats['video_frame_rate'] = $media['frame_rate'];
              if (isset($media['size'])) $stats['video_resolution'] = $media['size'];
              
              $debug = '';
              foreach($stats as $key => $val) $debug .= $key . '=' . $val . '; ';
              EncodingUtil::log(sprintf('Got stats for job %s: %s', $jobId, $debug), 'EDCEncodingController::jobStats', __LINE__);
            }
            else EncodingUtil::log(sprintf('Unable to invoke GetMediaInfo API - request successful, but "bitrate" not included in response', $input, $jobId), 'EDCEncodingController::jobStats', __LINE__, TRUE);
          }
          else EncodingUtil::log(sprintf('Unable to invoke GetMediaInfo API query'), 'EDCEncodingController::jobStats', __LINE__, TRUE);
        }
        else EncodingUtil::log(sprintf('Unable to invoke GetStatus API - request successful, but "job" for id %s not included in response', $input, $jobId), 'EDCEncodingController::jobStats', __LINE__, TRUE);
      }
      else EncodingUtil::log(sprintf('Unable to invoke GetStatus API query'), 'EDCEncodingController::jobStats', __LINE__, TRUE);
    }
    else EncodingUtil::log(sprintf('Invoked without specifying jobId'), 'EDCEncodingController::jobStats', __LINE__, TRUE);
    
    return count($stats) ? $stats : NULL;
  }
  
  /**
   * return TRUE if the designated storage service and region is the same as 
   * the encoding service
   * @param ObjectStorageController $storage_controller the storage controller
   * implementation for input and output files
   * @return boolean
   */
  protected function sameRegion(&$storage_controller) {
    return $storage_controller->getApi() == 's3' && $this->edc_region == $storage_controller->getApiRegion();
  }
  
  /**
   * return TRUE if the service region $region is valid, FALSE otherwise
   * @param string $region the (encoding) region to validate (may be NULL if
   * use did not specify an explicit region)
   * @return boolean
   */
  protected function validateServiceRegion($region) {
    return in_array($region, explode(',', self::ENCODING_COM_REGIONS));
  }
  
  /**
   * converts an xml string into an array
   * @param string $xml the xml to convert
   * @return array
   */
  private function xmlToArray($xml) {
    return json_decode(json_encode((array)simplexml_load_string($xml)), TRUE);
  }
  
}
?>
