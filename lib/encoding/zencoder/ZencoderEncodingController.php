<?php
/**
 * Implements testing functionality for the Zencoder platform
 */
class ZencoderEncodingController extends EncodingController {
  
  // default service region
  const DEFAULT_ZENCODER_REGION = 'us';
  // base API URL
  const ZENCODER_API_URL = 'https://app.zencoder.com/api/v2/';
  
  // credentials reference
  private $zencoder_credentials;
  // region
  private $zencoder_region;
  // strict mode?
  private $zencoder_strict_mode;
  // test mode?
  private $zencoder_test_mode;
  
  /**
   * invoked once during validation. Should return TRUE if authentication is 
   * successful, FALSE otherwise. this method may reference the optional 
   * attributes $service_key, $service_secret and $service_region as necessary 
   * to complete authentication
   * @return boolean
   */
  protected function authenticate() {
    $authenticated = FALSE;
    if ($response = $this->invokeApi('account')) {
      EncodingUtil::log(sprintf('Got account details. account_state: %s; plan: %s; minutes_used: %d; minutes_included: %d; billing_state: %s; integration_mode: %d', $response['account_state'], $response['plan'], $response['minutes_used'], $response['minutes_included'], $response['billing_state'], $response['integration_mode']), 'ZencoderEncodingController::authenticate', __LINE__);
      if ($authenticated = $response['account_state'] == 'active') {
        EncodingUtil::log(sprintf('Authentication successful'), 'ZencoderEncodingController::authenticate', __LINE__);
      }
      else EncodingUtil::log(sprintf('Authentication failed because account is not active'), 'ZencoderEncodingController::authenticate', __LINE__, TRUE);
    }
    else EncodingUtil::log(sprintf('Unable to authenticate'), 'ZencoderEncodingController::authenticate', __LINE__, TRUE);
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
    $jobId = NULL;
    if ($url = $this->getInput($storage_controller, $input)) {
      EncodingUtil::log(sprintf('Got input parameter %s for object %s', $url, $input), 'ZencoderEncodingController::encode', __LINE__);
      $job = array('input' => $url, 'download_connections' => $this->getNumberOfDownloaders($input_size), 'outputs' => array());
      if ($this->service_region && $this->service_region != self::DEFAULT_ZENCODER_REGION) $job['region'] = $this->service_region;
      if ($this->zencoder_test_mode) $job['test'] = TRUE;
      if ($this->zencoder_credentials && $this->zencoder_credentials != '1') $job['credentials'] = $this->zencoder_credentials;
      foreach($outputs as $i => $output) {
        $job['outputs'][$i]['url'] = $this->getInput($storage_controller, $output['output']);
        if ($this->zencoder_strict_mode) $job['outputs'][$i]['strict'] = TRUE;
        // change format for HLS jobs to ts (audio + video) or aac (audio only)
        $job['outputs'][$i]['format'] = $hls ? ($output['audio_only'] ? 'aac' : 'ts') : $format;
        if ($this->zencoder_credentials && $this->zencoder_credentials != '1') $job['outputs'][$i]['credentials'] = $this->zencoder_credentials;
        
        if ($audio_aac_profile != 'auto') $job['outputs'][$i]['max_aac_profile'] = $audio_aac_profile;
        $job['outputs'][$i]['audio_bitrate'] = $output['audio_bitrate'];
        $job['outputs'][$i]['audio_codec'] = $audio_codec;
        if ($audio_sample_rate != 'auto') $job['outputs'][$i]['audio_sample_rate'] = $audio_sample_rate;
        
        // for HLS set type to segmented
        if ($hls) $job['outputs'][$i]['type'] = 'segmented';
        
        // video specific settings
        if (!$output['audio_only']) {
          if ($bframes) $job['h264_bframes'] = $bframes;
          if (isset($output['h264_profile'])) $job['h264_profile'] = $output['h264_profile'];
          if ($reference_frames) $job['h264_reference_frames'] = $reference_frames;
          if (isset($output['keyframe'])) $job['keyframe_rate'] = $output['keyframe'];
          if (isset($output['frame_rate'])) $job['outputs'][$i]['max_frame_rate'] = $output['frame_rate'];
          $job['one_pass'] = !$two_pass;
          if ($hls && $hls_segment) $job['segment_seconds'] = $hls_segment;
          if (isset($output['video_bitrate'])) $job['outputs'][$i]['video_bitrate'] = $output['video_bitrate'];
          if ($video_codec) $job['outputs'][$i]['video_codec'] = $video_codec;
          if (isset($output['width'])) $job['outputs'][$i]['width'] = $output['width'];
        }
        ksort($job['outputs'][$i]);
        $debug = '';
        foreach($job['outputs'][$i] as $key => $val) $debug .= $key . '=' . $val . '; ';
        EncodingUtil::log(sprintf('Added encoding output: %s', $debug), 'ZencoderEncodingController::encode', __LINE__);
      }
      ksort($job);
      $debug = '';
      foreach($job as $key => $val) if (!is_array($val)) $debug .= $key . '=' . $val . '; ';
      EncodingUtil::log(sprintf('Submitting encoding job: %s', $debug), 'ZencoderEncodingController::encode', __LINE__);
      
      // submit job via API
      if ($response = $this->invokeApi('jobs', 'POST', $job)) {
        if (isset($response['outputs']) && is_array($response['outputs']) && count($response['outputs'])) {
          EncodingUtil::log(sprintf('Job submitted successfully - job ID %s, outputs %d', $response['id'], count($response['outputs'])), 'ZencoderEncodingController::encode', __LINE__);
          $jobId = $response['id'];
          if (count($response['outputs']) != count($job['outputs'])) EncodingUtil::log(sprintf('Warning: Number of job outputs %d does not match number of requested outputs %d', count($response['outputs']), count($job['outputs'])), 'ZencoderEncodingController::encode', __LINE__, TRUE);
        }
        else EncodingUtil::log(sprintf('Job %s submitted - but did not include any outputs', $response['id']), 'ZencoderEncodingController::encode', __LINE__, TRUE);
      }
      else EncodingUtil::log(sprintf('Unable to submit job'), 'ZencoderEncodingController::encode', __LINE__, TRUE);
    }
    else EncodingUtil::log(sprintf('Unable to get input parameter for object %s', $input), 'ZencoderEncodingController::encode', __LINE__, TRUE);
    
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
      $url = $this->getUrl('jobs');
      $requests = array();
      $headers = $this->getHeaders();
      foreach($jobIds as $i => $jobId) $requests[$i] = array('url' => $this->getUrl('progress', sprintf('jobs/%s/', $jobId)), 'headers' => $headers);
      if ($result = EncodingUtil::curl($requests, TRUE)) {
        foreach($jobIds as $i => $jobId) {
          if ($result['status'][$i] >= 200 && $result['status'][$i] < 300) {
            EncodingUtil::log(sprintf('Status request for job %s successful - status code %d', $jobId, $result['status'][$i]), 'ZencoderEncodingController::getJobStatus', __LINE__);
            if (isset($result['body'][$i]) && ($response = json_decode($result['body'][$i], TRUE))) {
              if (isset($response['state']) && isset($response['input']['state']) && isset($response['outputs']) && is_array($response['outputs']) && count($response['outputs'])) {
                $num_outputs = count($response['outputs']);
                $state = $response['state'];
                $input_state = $response['input']['state'];
                $output_states = array();
                $output_progress = array();
                $output_failed = 0;
                $output_success = 0;
                $output_uploading = 0;
                $output_queued = 0;
                foreach($response['outputs'] as $output) {
                  $state = trim(strtolower($output['state']));
                  $output_states[$state] = TRUE;
                  if (isset($output['current_event'])) {
                    $event = trim(strtolower($output['current_event']));
                    if ($event == 'uploading') $output_uploading++;
                  }
                  if (isset($output['current_event_progress'])) $output_progress[] = $output['current_event_progress'];
                  if ($state == 'queued' || $state == 'assigning') $output_queued++;
                  else if ($state == 'finished') $output_success++;
                  else if ($state == 'failed') $output_failed++;
                }
                $output_states = array_keys($output_states);
                EncodingUtil::log(sprintf('Job State %s - Overall: %s; Input: %s; Outputs: %s; Progress: %s; Queued: %d; Uploading: %d; Pending: %d; Success: %d; Failed: %d', $jobId, $state, $input_state, implode(', ', $output_states), implode(', ', $output_progress), $output_queued, $output_uploading, $num_outputs - $output_success - $output_failed, $output_success, $output_failed), 'ZencoderEncodingController::getJobStatus', __LINE__);
                
                // downloading
                if ($input_state == 'queued' || $input_state == 'processing' || isset($response['input']['current_event'])) $status[$jobId] = 'download';
                // queued
                else if ($output_queued) $status[$jobId] = 'queue';
                // encoding
                else if (in_array('processing', $output_states) && $output_uploading != ($num_outputs - $output_success - $output_failed)) $status[$jobId] = 'encode';
                // uploading
                else if (in_array('processing', $output_states)) $status[$jobId] = 'upload';
                // completed successfully
                else if ($state == 'finished') $status[$jobId] = 'success';
                // failed
                else if ($state == 'failed' || $state == 'cancelled') $status[$jobId] = $output_success ? 'partial' : 'fail';
                // this should cover all scenarios - otherwise something is wrong
                
                if (isset($status[$jobId])) {
                  EncodingUtil::log(sprintf('Status of job %s is %s', $jobId, $status[$jobId]), 'ZencoderEncodingController::getJobStatus', __LINE__);
                }
                else EncodingUtil::log(sprintf('Failed to determine status of job %s', $jobId), 'ZencoderEncodingController::getJobStatus', __LINE__, TRUE);
              }
              else {
                EncodingUtil::log(sprintf('Status response for job %s did not include state, input or outputs: %s. Setting job status to fail', $jobId, $result['body'][$i]), 'ZencoderEncodingController::getJobStatus', __LINE__, TRUE);
                $status[$jobId] = 'fail';
              }
            }
            else {
              EncodingUtil::log(sprintf('Status request for job %s did not return a valid json response: %s. Setting job status to fail', $jobId, $result['body'][$i]), 'ZencoderEncodingController::getJobStatus', __LINE__, TRUE);
              $status[$jobId] = 'fail';
            }
          }
          else {
            EncodingUtil::log(sprintf('Unable to get status for job %s - status code %d. Setting job status to fail', $jobId, $result['status'][$i]), 'ZencoderEncodingController::getJobStatus', __LINE__, TRUE);
            $status[$jobId] = 'fail';
          } 
        }
      }
      else EncodingUtil::log(sprintf('Unable to invoke job status API request'), 'ZencoderEncodingController::getJobStatus', __LINE__, TRUE);
    }
    else EncodingUtil::log(sprintf('Invoked without specifying any jobIds'), 'ZencoderEncodingController::getJobStatus', __LINE__, TRUE);
    
    return count($status) ? $status : NULL;
  }
  
  /**
   * returns a hash containing 2 values:
   *   service:        the storage service corresponding to the selected 
   *                   zencoder region
   *   region:         the storage region corresponding to the selected 
   *                   zencoder region
   * returns NULL if the designated service region is invalid
   * @param string $service_region optional service region identifier. If 
   * not specified, $this->service_region will be assumed
   * @return array
   */
  private function getStorageRegion($service_region=NULL) {
    $service_region = $service_region ? $service_region : $this->zencoder_region;
    $region = array('service' => 's3');
    switch($service_region) {
      case 'us':
      case 'us-virginia':
        $region['region'] = 'us-east-1';
        break;
      case 'europe':
      case 'eu-dublin':
        $region['region'] = 'eu-west-1';
        break;
      case 'asia':
      case 'asia-singapore':
        $region['region'] = 'ap-southeast-1';
        break;
      case 'sa':
      case 'sa-saopaulo':
        $region['region'] = 'sa-east-1';
        break;
      case 'australia':
      case 'australia-sydney':
        $region['region'] = 'ap-southeast-2';
        break;
      case 'us-oregon':
        $region['region'] = 'us-west-2';
        break;
      case 'us-n-california':
        $region['region'] = 'us-west-1';
        break;
      case 'asia-tokyo':
        $region['region'] = 'ap-northeast-1';
        break;
      case 'us-central-gce':
        $region['service'] = 'gcs';
        $region['region'] = 'us-central1';
        break;
      case 'eu-west-gce':
        $region['service'] = 'gcs';
        $region['region'] = 'europe-west1';
        break;
    }
    return isset($region['region']) ? $region : NULL;
  }
  
  /**
   * returns the base headers to use for an API call
   * @param string $method the HTTP method
   * @return array
   */
  private function getHeaders($method='GET') {
    $headers = array('Zencoder-Api-Key' => $this->service_key);
    if ($method == 'POST' || $method == 'PUT') $headers['Content-Type'] = 'application/json';
    return $headers;
  }
  
  /**
   * returns the input identifier based on the parameters specified
   * @param ObjectStorageController $storage_controller the storage controller
   * implementation for input and output files
   * @param string $object object name within the storage container
   * @return string
   */
  private function getInput(&$storage_controller, $object) {
    $url = NULL;
    switch($storage_controller->getApi()) {
      case 's3':
        $url = 's3://';
        break;
      case 'gcs':
        $url = 'gcs://';
        break;
    }
    if ($object) {
      if (!$this->zencoder_credentials) $url .= urlencode($storage_controller->getApiKey()) . ':' . urlencode($storage_controller->getApiSecret()) . '@';
      $url .= $storage_controller->getContainer() . '/' . $object;
    }
    return $url;
  }
  
  /**
   * returns the API URL for the designated $action
   * @param string $action the action to return the URL for
   * @param string $prefix optional URL prefix (should end with /)
   * @return string
   */
  private function getUrl($action, $prefix=NULL) {
    return sprintf('%s%s%s.json', self::ZENCODER_API_URL, $prefix ? $prefix : '', $action);
  }
  
  /**
   * may be used to perform pre-usage initialization. return TRUE on success, 
   * FALSE on failure
   * @return boolean
   */
  protected function init() {
    $this->zencoder_credentials = getenv('bm_param_service_param1');
    $this->zencoder_region = $this->service_region ? $this->service_region : self::DEFAULT_ZENCODER_REGION;
    $this->zencoder_test_mode = getenv('bm_param_service_param2') == '1';
    $this->zencoder_strict_mode = getenv('bm_param_service_param3') == '1';
    return TRUE;
  }
  
  /**
   * invokes an API action and returns the response. Returns FALSE if the call 
   * results in a non 2xx status code. Returns an object if the response 
   * provides one in the body, otherwise returns TRUE. Returns NULL on error
   * @param string $action the API action to invoke
   * @param string $method the HTTP method to use - default is GET
   * @param object/array $body optional object to include in the request body 
   * (json formatted) - for non PUT and POST requests only
   * @return mixed
   */
  private function invokeApi($action, $method='GET', $body=NULL) {
    $response = NULL;
    $request = array('method' => $method, 'url' => $this->getUrl($action), 'headers' => $this->getHeaders($method));
    if ($body && ($method == 'POST' || $method == 'PUT')) $request['body'] = json_encode($body);
    if ($result = EncodingUtil::curl(array($request), TRUE)) {
      if ($result['status'][0] >= 200 && $result['status'][0] < 300) {
        EncodingUtil::log(sprintf('%s %s completed successfully with status code %d', $method, $action, $result['status'][0]), 'ZencoderEncodingController::invokeApi', __LINE__);
        if (!$result['body'][0] || !($response = json_decode($result['body'][0], TRUE))) $response = TRUE;
      }
      else {
        EncodingUtil::log(sprintf('%s %s resulting in status code %d', $method, $action, $result['status'][0]), 'ZencoderEncodingController::invokeApi', __LINE__, TRUE);
        $response = FALSE;
      }
    }
    else EncodingUtil::log(sprintf('%s %s resulting could not be invoked', $method, $action), 'ZencoderEncodingController::invokeApi', __LINE__, TRUE);
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
      $request = array('url' => $this->getUrl($jobId, 'jobs/'), 'headers' => $this->getHeaders());
      if ($result = EncodingUtil::curl(array($request), TRUE)) {
        if ($result['status'][0] >= 200 && $result['status'][0] < 300) {
          $response = json_decode($result['body'][0], TRUE);
          if (isset($response['job']['input_media_file'])) {
            EncodingUtil::log(sprintf('Successfully retrieved job stats for job %s', $jobId), 'ZencoderEncodingController::jobStats', __LINE__);
            if (isset($response['job']['input_media_file']['audio_bitrate_in_kbps'])) $stats['audio_bit_rate'] = $response['job']['input_media_file']['audio_bitrate_in_kbps']*1;
            if (isset($response['job']['input_media_file']['audio_sample_rate'])) $stats['audio_sample_rate'] = $response['job']['input_media_file']['audio_sample_rate']*1;
            if (isset($response['job']['input_media_file']['channels'])) $stats['audio_channels'] = $response['job']['input_media_file']['channels']*1;
            if (isset($response['job']['input_media_file']['audio_codec'])) $stats['audio_codec'] = $response['job']['input_media_file']['audio_codec'];
            if (isset($response['job']['input_media_file']['duration_in_ms'])) $stats['duration'] = $response['job']['input_media_file']['duration_in_ms']/1000;
            if (isset($response['job']['input_media_file']['total_bitrate_in_kbps'])) $stats['total_bit_rate'] = $response['job']['input_media_file']['total_bitrate_in_kbps']*1;
            if (isset($response['job']['input_media_file']['video_bitrate_in_kbps'])) $stats['video_bit_rate'] = $response['job']['input_media_file']['video_bitrate_in_kbps']*1;
            if (isset($response['job']['input_media_file']['video_codec'])) $stats['video_codec'] = $response['job']['input_media_file']['video_codec'];
            if (isset($response['job']['input_media_file']['frame_rate'])) $stats['video_frame_rate'] = $response['job']['input_media_file']['frame_rate']*1;
            if (isset($response['job']['input_media_file']['width']) && isset($response['job']['input_media_file']['height'])) $stats['video_resolution'] = sprintf('%dx%d', $response['job']['input_media_file']['width'], $response['job']['input_media_file']['height']);
            // determine job_start, job_stop, job_time, error, output_failed, output_success
            
            // job_start - earliest of created_at and submitted_at
            $errors = array();
            $output_failed = 0;
            $output_success = 0;
            if (isset($response['job']['input_media_file']['error_message'])) $errors[] = $response['job']['input_media_file']['error_message'];
            $created_at = isset($response['job']['created_at']) ? strtotime($response['job']['created_at']) : NULL;
            $submitted_at = isset($response['job']['submitted_at']) ? strtotime($response['job']['submitted_at']) : NULL;
            if ($created_at || $submitted_at) $stats['job_start'] = $created_at < $submitted_at ? $created_at : $submitted_at;
            if (isset($stats['job_start'])) EncodingUtil::log(sprintf('Job %s created_at=%s; submitted_at=%s; job_start=%s', $jobId, $created_at, $submitted_at, $stats['job_start']), 'ZencoderEncodingController::jobStats', __LINE__);
            else EncodingUtil::log(sprintf('Unable to determine created_at or submitted_at for job %s', $jobId), 'ZencoderEncodingController::jobStats', __LINE__, TRUE);
            
            // job_stop - latest of finished_at for job and for all outputs
            // output_audio_bit_rate, output_audio_channels, output_audio_codecs, output_durations
            // output_formats, output_video_bit_rates, output_video_codecs, output_video_frame_rates
            // output_video_resolutions
            $stats['output_audio_bit_rate'] = array();
            $stats['output_audio_channels'] = array();
            $stats['output_audio_codecs'] = array();
            $stats['output_audio_bit_rate'] = array();
            $stats['output_audio_sample_rates'] = array();
            $stats['output_durations'] = array();
            $stats['output_formats'] = array();
            $stats['output_total_bit_rates'] = array();
            $stats['output_video_bit_rates'] = array();
            $stats['output_video_codecs'] = array();
            $stats['output_video_frame_rates'] = array();
            $stats['output_video_resolutions'] = array();
            $finished_at = array();
            if (isset($response['job']['finished_at'])) $finished_at[] = strtotime($response['job']['finished_at']);
            if (isset($response['job']['output_media_files']) && is_array($response['job']['output_media_files']) && count($response['job']['output_media_files'])) {
              foreach($response['job']['output_media_files'] as $output) {
                if (isset($output['finished_at'])) $finished_at[] = strtotime($output['finished_at']);
                if (isset($output['error_message']) && !in_array($output['error_message'], $errors)) $errors[] = $output['error_message'];
                $output['state'] == 'failed' ? $output_failed++ : $output_success++;
                
                // output status
                if (isset($output['audio_bitrate_in_kbps'])) $stats['output_audio_bit_rate'][] = $output['audio_bitrate_in_kbps']*1;
                if (isset($output['audio_sample_rate'])) $stats['output_audio_sample_rates'][] = $output['audio_sample_rate']*1;
                if (isset($output['channels'])) $stats['output_audio_channels'][] = $output['channels']*1;
                if (isset($output['audio_codec'])) $stats['output_audio_codecs'][] = $output['audio_codec'];
                if (isset($output['duration_in_ms'])) $stats['output_durations'][] = $output['duration_in_ms']/1000;
                if (isset($output['format'])) $stats['output_formats'][] = $output['format'];
                if (isset($output['total_bitrate_in_kbps'])) $stats['output_total_bit_rates'][] = $output['total_bitrate_in_kbps']*1;
                if (isset($output['video_bitrate_in_kbps'])) $stats['output_video_bit_rates'][] = $output['video_bitrate_in_kbps']*1;
                if (isset($output['video_codec'])) $stats['output_video_codecs'][] = $output['video_codec'];
                if (isset($output['frame_rate'])) $stats['output_video_frame_rates'][] = $output['frame_rate']*1;
                if (isset($output['width']) && isset($output['height'])) $stats['output_video_resolutions'][] = sprintf('%dx%d', $output['width'], $output['height']);
              }
            }
            sort($finished_at);
            if (count($finished_at)) {
              EncodingUtil::log(sprintf('Got %d finished_at times for job %s', count($finished_at), $jobId), 'ZencoderEncodingController::jobStats', __LINE__);
              $stats['job_stop'] = $finished_at[count($finished_at) - 1];
            }
            else EncodingUtil::log(sprintf('Unable to determine any finished_at times for job %s', $jobId), 'ZencoderEncodingController::jobStats', __LINE__, TRUE);
            
            // job_time: job_stop - job_start
            if (isset($stats['job_start']) && isset($stats['job_stop'])) $stats['job_time'] = $stats['job_stop'] - $stats['job_start'];
            
            // error message
            if ($errors) $stats['error'] = implode('; ', $errors);
            
            // output_failed, output_success
            $stats['output_failed'] = $output_failed;
            $stats['output_success'] = $output_success;
            
            // purge empty stats and print debug
            $debug = '';
            foreach($stats as $key => $val) {
              if (is_array($val) && !count($val)) unset($stats[$key]);
              else {
                if (is_array($val)) $stats[$key] = implode(',', $val);
                $debug .= $key . '=' . $stats[$key] . '; ';
              }
            }
            EncodingUtil::log(sprintf('Got stats for job %s: %s', $jobId, $debug), 'ZencoderEncodingController::jobStats', __LINE__);
          }
          else EncodingUtil::log(sprintf('Job stats API response for %s does not have the necessary input_media_file response key', $jobId), 'ZencoderEncodingController::jobStats', __LINE__, TRUE);
        }
        else EncodingUtil::log(sprintf('Unable to invoke job stats API request for job ID - status %d', $jobId, $result['status'][0]), 'ZencoderEncodingController::jobStats', __LINE__, TRUE);
      }
      else EncodingUtil::log(sprintf('Unable to invoke job stats API request for job ID', $jobId), 'ZencoderEncodingController::jobStats', __LINE__, TRUE);
    }
    else EncodingUtil::log(sprintf('Invoked without specifying jobId'), 'ZencoderEncodingController::jobStats', __LINE__, TRUE);
    
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
    $region = $this->getStorageRegion();
    return $region && $region['service'] == $storage_controller->getApi() && $region['region'] == $storage_controller->getApiRegion();
  }
  
  /**
   * return TRUE if the service region $region is valid, FALSE otherwise
   * @param string $region the (encoding) region to validate (may be NULL if
   * use did not specify an explicit region)
   * @return boolean
   */
  protected function validateServiceRegion($region) {
    return $this->getStorageRegion($region) ? TRUE : FALSE;
  }
  
}
?>
