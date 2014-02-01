<?php
/**
 * Implements testing functionality for the AWS Elastic Transcoder platform
 */
class AWSEncodingController extends EncodingController {
  
  // default service region
  const DEFAULT_AWS_REGION = 'us-east-1';
  // API hash algorithm
  const AWS_API_ALGORITHM = 'AWS4-HMAC-SHA256';
  // date header
  const AWS_API_DATE_HEADER = 'x-amz-date';
  // base API URL
  const AWS_API_HOST = 'elastictranscoder.{region}.amazonaws.com';
  // max requests/second for read job
  const AWS_API_MAX_READ_JOB_REQUESTS_SEC = 4;
  // api request type identifier
  const AWS_API_REQUEST_ID = 'aws4_request';
  // service identifier
  const AWS_API_SERVICE_ID = 'elastictranscoder';
  // api version
  const AWS_API_VERSION = '2012-09-25';
  // default audio bitrate (this value is required)
  const AWS_DEFAULT_AUDIO_BITRATE = 64;
  // default h.264 profile
  const AWS_DEFAULT_H264_PROFILE = 'baseline';
  // hash algorithm
  const AWS_HASH_ALGORITHM = 'sha256';
  // jobs resource URI
  const AWS_JOBS_RESOURCE = '/2012-09-25/jobs';
  // padding policy
  const AWS_PADDING_POLICY = 'NoPad';
  // pipelines resource URI
  const AWS_PIPELINES_RESOURCE = '/2012-09-25/pipelines';
  // all pipelines identifier for service_param1
  const AWS_PIPELINE_ALL = '_all_';
  // first pipeline identifier for service_param1
  const AWS_PIPELINE_FIRST = '_first_';
  // stores information about presets created during testing
  const AWS_PRESETS_FILE = '.aws-presets';
  // presets resource URI
  const AWS_PRESETS_RESOURCE = '/2012-09-25/presets';
  // valid region identifiers
  const AWS_REGIONS = 'us-east-1,us-west-1,us-west-2,eu-west-1,ap-southeast-1,ap-southeast-2,ap-northeast-1,sa-east-1';
  // date format for request headers
  const AWS_SIGNATURE_DATE_FORMAT = 'Ymd\THis\Z';
  // sizing policy for video and thumbnails
  const AWS_SIZING_POLICY = 'ShrinkToFit';
  // thumbnail settings - not used for testing, but required by the API
  const AWS_THUMBNAILS_INTERVAL = 6000;
  const AWS_THUMBNAIL_RESOLUTION = '640x480';
  
  // AWS API hostname
  private $aws_host;
  // Pipelines to use for jobs
  private $aws_job_pipelines;
  // Pointer to the next job pipeline
  private $aws_job_pipelines_ptr = 0;
  // Pipeline identifier
  private $aws_pipeline;
  // Presets
  private $aws_presets;
  // AWS region
  private $aws_region;
  
  /**
   * invoked once during validation. Should return TRUE if authentication is 
   * successful, FALSE otherwise. this method may reference the optional 
   * attributes $service_key, $service_secret and $service_region as necessary 
   * to complete authentication
   * @return boolean
   */
  protected function authenticate() {
    // authentication was successful if aws_job_pipelines is an array (see 
    // init method)
    return is_array($this->aws_job_pipelines);
  }
  
  /**
   * invoked following test completion. May be used to perform cleanup tasks.
   * Should return TRUE on success, FALSE on failure
   * @return boolean
   */
  protected function cleanupService() {
    $success = TRUE;
    if (file_exists($presetsFile = $this->getPresetsFile())) {
      $deleted = 0;
      $presets = array();
      foreach(file($presetsFile) as $presetId) {
        if ($presetId = trim($presetId)) {
          $presets[] = $presetId;
          if ($response = $this->invokeApi(sprintf('%s/%s', self::AWS_PRESETS_RESOURCE, $presetId), 'DELETE')) {
            EncodingUtil::log(sprintf('Preset %s deleted successfully', $presetId), 'AWSEncodingController::cleanupService', __LINE__);
            $deleted++;
          }
          else EncodingUtil::log(sprintf('Unable to invoke DELETE preset API request for %s', $presetId), 'AWSEncodingController::cleanupService', __LINE__, TRUE);
        }
      }
      $success = count($presets) == $deleted;
      EncodingUtil::log(sprintf('%d of %d presets deleted', $deleted, count($presets)), 'AWSEncodingController::cleanupService', __LINE__, !$success);
    }
    else EncodingUtil::log(sprintf('No service cleanup necessary because preset file %s does not exist', $presetsFile), 'AWSEncodingController::cleanupService', __LINE__);
    
    return $success;
  }
  
  /**
   * compares the properties of a job preset with those of an existing preset
   * returns TRUE if they match, FALSE otherwise
   * @param array $jobPreset the desired job preset properties
   * @param array $preset the existing preset
   * @return boolean
   */
  private function comparePresets($jobPreset, $preset) {
    $matched = TRUE;
    foreach($jobPreset as $key => $val) {
      if (isset($preset[$key])) {
        $matched = is_array($val) ? $this->comparePresets($val, $preset[$key]) : trim(strtolower($val)) == trim(strtolower($preset[$key]));
      }
      else $matched = FALSE;
      
      if (!$matched) break;
    }
    // audio only
    if (isset($preset['Video']) && !isset($jobPreset['Video'])) $matched = FALSE;
    
    return $matched;
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
    
    // determine pipeline to use
    $keys = array_keys($this->aws_job_pipelines);
    $pipeline_id = $keys[$this->aws_job_pipelines_ptr];
    $pipeline_name = $this->aws_job_pipelines[$pipeline_id];
    
    if ($pipeline_id) {
      $job = array('Input' => array('Key' => $input), 'Outputs' => array(), 'PipelineId' => $pipeline_id);
      foreach($outputs as $output) {
        if ($presetId = $this->getPresetId($format, $audio_aac_profile, $audio_codec, $audio_sample_rate, $video_codec, $bframes, $reference_frames, $two_pass, $hls, $hls_segment, $output['audio_bitrate'], $output['audio_only'], isset($output['frame_rate']) ? $output['frame_rate'] : NULL, isset($output['h264_profile']) ? $output['h264_profile'] : NULL, isset($output['keyframe']) ? $output['keyframe'] : NULL, isset($output['video_bitrate']) ? $output['video_bitrate'] : NULL, isset($output['width']) ? $output['width'] : NULL)) {
          EncodingUtil::log(sprintf('Got preset %s for input %s output %s - adding to job outputs', $presetId, $input, $output['output']), 'AWSEncodingController::encode', __LINE__);
          $job_output = array('Key' => $output['output'], 'ThumbnailPattern' => '', 'PresetId' => $presetId);
          if ($hls) {
            $job_output['Key'] = str_replace('.m3u8', '', $job_output['Key']);
            $job_output['SegmentDuration'] = $hls_segment;
          }
          $job['Outputs'][] = $job_output;
        }
        else {
          EncodingUtil::log(sprintf('Unable to get preset for job output %s - aborting job', $output['output']), 'AWSEncodingController::encode', __LINE__, TRUE);
          $job = NULL;
          break;
        }
      }
      if ($job) {
        EncodingUtil::log(sprintf('Attempting to create job: %s', $this->apiJsonEncode($job)), 'AWSEncodingController::encode', __LINE__);
        if ($response = $this->invokeApi(self::AWS_JOBS_RESOURCE, 'POST', NULL, $job)) {
          if (isset($response['Job']['Id'])) {
            $jobId = $response['Job']['Id'];
            EncodingUtil::log(sprintf('Encode job started successfully - job ID %s', $jobId), 'AWSEncodingController::encode', __LINE__);
          }
          else EncodingUtil::log(sprintf('Create job API POST successful - but did not include Job => Id value'), 'AWSEncodingController::encode', __LINE__, TRUE);
        }
        else EncodingUtil::log(sprintf('Unable to invoke create job API POST'), 'AWSEncodingController::encode', __LINE__, TRUE);
      }
    }
    else EncodingUtil::log(sprintf('Unable to initiate encoding - no pipeline_id exists'), 'AWSEncodingController::encode', __LINE__, TRUE);
    
    // increment pipeline pointer
    if ($jobId) {
      $this->aws_job_pipelines_ptr++;
      if ($this->aws_job_pipelines_ptr >= count($this->aws_job_pipelines)) $aws_job_pipelines_ptr = 0;
    }
    
    return $jobId;
  }
  
  /**
   * returns the headers for an API request including signing of the request
   * @param string $uri the request URI
   * @param string $method the http method
   * @param array $params optional hash of query parameters
   * @param mixed $body optional body for the API request. may be a string or
   * an array representing a form
   * @return array
   */
  protected function getHeaders($uri, $method='GET', $params=NULL, $body=NULL) {
    $headers = array('host' => $this->aws_host);
    $headers[self::AWS_API_DATE_HEADER] = gmdate(self::AWS_SIGNATURE_DATE_FORMAT);
    $headers['Authorization'] = $this->sign($headers, $uri, $method, $params, $body);
    return $headers;
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
   * return NULL on error (test will abort)
   * @return array
   */
  protected function getJobStatus($jobIds) {
    $status = array();
    if (is_array($jobIds) && count($jobIds)) {
      // create requests
      $requests = array();
      foreach($jobIds as $jobId) {
        $uri = $uri = sprintf('%s/%s', self::AWS_JOBS_RESOURCE, $jobId);
        $requests[] = array('url' => $this->getUrl($uri), 'headers' => $this->getHeaders($uri));
      }
      
      // limit # of requests/second
      $reset_max_api_requests_sec = NULL;
      if (!getenv('bm_param_max_api_requests_sec') || getenv('bm_param_max_api_requests_sec') > self::AWS_API_MAX_READ_JOB_REQUESTS_SEC) {
        $reset_max_api_requests_sec = getenv('bm_param_max_api_requests_sec');
        putenv('bm_param_max_api_requests_sec', self::AWS_API_MAX_READ_JOB_REQUESTS_SEC);
        EncodingUtil::log(sprintf('Setting max_api_requests_sec to %d from %d', self::AWS_API_MAX_READ_JOB_REQUESTS_SEC, $reset_max_api_requests_sec), 'AWSEncodingController::getJobStatus', __LINE__);
        $reset_max_api_requests_sec = TRUE;
      }
      
      // initiate parallel requests to get job status
      if ($result = EncodingUtil::curl($requests, TRUE)) {
        foreach($jobIds as $i => $jobId) {
          if ($result['status'][$i] >= 200 && $result['status'][$i] < 300) {
            EncodingUtil::log(sprintf('Status request for job %s successful - status code %d', $jobId, $result['status'][$i]), 'AWSEncodingController::getJobStatus', __LINE__);
            if (isset($result['body'][$i]) && ($response = json_decode($result['body'][$i], TRUE))) {
              if (isset($response['Job']) && isset($response['Job']['Status']) && isset($response['Job']['Outputs']) && is_array($response['Job']['Outputs'])) {
                $output_queued = 0;
                $output_encoding = 0;
                $output_complete = 0;
                $output_failed = 0;
                foreach($response['Job']['Outputs'] as $i => $output) {
                  EncodingUtil::log(sprintf('Status of job %s output %d is %s', $jobId, $i+1, $output['Status']), 'AWSEncodingController::getJobStatus', __LINE__);
                  if ($output['Status'] == 'Submitted') $output_queued++;
                  // API documentation lists 'In Progress' as a valid status - but the correct status reported appears to be 'Progressing'
                  else if ($output['Status'] == 'In Progress' || $output['Status'] == 'Progressing') $output_encoding++;
                  else if ($output['Status'] == 'Complete') $output_complete++;
                  else $output_failed;
                }
                
                if ($output_encoding) $status[$jobId] = 'encode';
                else if ($output_queued) $status[$jobId] = 'queue';
                else if ($output_complete && $output_failed) $status[$jobId] = 'partial';
                else if ($output_complete) $status[$jobId] = 'success';
                else $status[$jobId] = 'fail';
                
                EncodingUtil::log(sprintf('Set status of job %s to %s using API status %s. Outputs queued: %d; encoding %d; complete %d; failed %d', $jobId, $status[$jobId], $response['Job']['Status'], $output_queued, $output_encoding, $output_complete, $output_failed), 'AWSEncodingController::getJobStatus', __LINE__);
              }
              else {
                EncodingUtil::log(sprintf('Read job response for job %s did not include Job => Status or Job => Outputs. Setting job status to fail', $jobId), 'AWSEncodingController::getJobStatus', __LINE__, TRUE);
                $status[$jobId] = 'fail';
              }
            }
            else {
              EncodingUtil::log(sprintf('Read job response for job %s is not valid json: %s. Setting job status to fail', $jobId, $result['body'][$i]), 'AWSEncodingController::getJobStatus', __LINE__, TRUE);
              $status[$jobId] = 'fail';
            }
          }
          else {
            EncodingUtil::log(sprintf('Unable to get status for job %s - status code %d. Setting job status to fail', $jobId, $result['status'][$i]), 'AWSEncodingController::getJobStatus', __LINE__, TRUE);
            $status[$jobId] = 'fail';
          } 
        }
      }
      else EncodingUtil::log(sprintf('Unable to invoke job status API request'), 'AWSEncodingController::getJobStatus', __LINE__, TRUE);
      
      if ($reset_max_api_requests_sec) {
        EncodingUtil::log(sprintf('Resetting max_api_requests_sec to %s', $reset_max_api_requests_sec === TRUE ? 'NULL' : $reset_max_api_requests_sec), 'AWSEncodingController::getJobStatus', __LINE__);
        putenv('bm_param_max_api_requests_sec', $reset_max_api_requests_sec === TRUE ? NULL : $reset_max_api_requests_sec);
      }
    }
    else EncodingUtil::log(sprintf('Invoked without specifying any jobIds'), 'AWSEncodingController::getJobStatus', __LINE__, TRUE);
    
    return count($status) ? $status : NULL;
  }
  
  /**
   * returns a hash representing the ids/names of all pipelines associated with 
   * the AWS account that are in 'Active' status and whose input and output 
   * buckets correspond with the 'storage_container' parameter. Returns an 
   * empty array if the account has no such pipelines. Returns NULL on error
   */
  private function getPipelines() {
    $pipelines = NULL;
    if ($response = $this->invokeApi(self::AWS_PIPELINES_RESOURCE)) {
      if (isset($response['Pipelines']) && is_array($response['Pipelines'])) {
        $pipelines = array();
        EncodingUtil::log(sprintf('Get pipelines API request successful - account has %d pipelines', count($response['Pipelines'])), 'AWSEncodingController::getPipelines', __LINE__);
        $bucket = getenv('bm_param_storage_container');
        foreach($response['Pipelines'] as $pipeline) {
          $id = $pipeline['Id'] . '/' . $pipeline['Name'];
          $input_bucket = $pipeline['InputBucket'];
          $output_bucket = $pipeline['OutputBucket'];
          if ($pipeline['Status'] == 'Active') {
            if ($bucket == $input_bucket && $bucket == $output_bucket) {
              EncodingUtil::log(sprintf('Including pipeline %s with input and output buckets %s', $id, $bucket), 'AWSEncodingController::getPipelines', __LINE__);
              $pipelines[$pipeline['Id']] = $pipeline['Name'];
            }
            else EncodingUtil::log(sprintf('Skipping pipeline %s because InputBucket %s or OutputBucket %s is not the same as the storage_container parameter %s', $id, $input_bucket, $output_bucket, $bucket), 'AWSEncodingController::getPipelines', __LINE__);
          }
          else EncodingUtil::log(sprintf('Skipping pipeline %s because status is not Active', $id), 'AWSEncodingController::getPipelines', __LINE__);
        }
      }
      else EncodingUtil::log(sprintf('Get pipelines API request successful - but failed to include "Pipelines" attribute in the response'), 'AWSEncodingController::getPipelines', __LINE__, TRUE);
    }
    else EncodingUtil::log(sprintf('Unable to invoke get pipelines API request to retrieve pipelines'), 'AWSEncodingController::getPipelines', __LINE__, TRUE);
    return $pipelines;
  }
  
  /**
   * returns the path to the file used to record objects created by test jobs
   * @return string
   */
  private final function getPresetsFile() {
    return sprintf('%s/%s', getenv('bm_run_dir'), self::AWS_PRESETS_FILE);
  }
  
  /**
   * returns the preset ID for the specified output settings. This method 
   * creates the preset if it does not already exist
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
   * @param int $audio_bitrate desired audio bitrate
   * @param boolean $audio_only TRUE for audio only output
   * @param int $frame_rate optional specific frame rate to target
   * @param string $h264_profile h264 profile - one of: 
   * baseline - Baseline, 3.0; main - Main, 3.1; high - High
   * @param int $keyframe number of keyframes per second
   * @param int $video_bitrate desired video bitrate
   * @param int $width desired video resolution width (height should be 
   * adjusted accordingly)
   * @return string
   */
  private final function getPresetId($format, $audio_aac_profile, $audio_codec, $audio_sample_rate, $video_codec, $bframes, $reference_frames, $two_pass, $hls, $hls_segment, $audio_bitrate, $audio_only, $frame_rate, $h264_profile, $keyframe, $video_bitrate, $width) {
    $presetId = NULL;
    // load existing account presets
    if (!isset($this->aws_presets)) $this->aws_presets = $this->getPresets();
    
    if (is_array($this->aws_presets)) {
      EncodingUtil::log(sprintf('Looking up preset for values - format: %s; audio_aac_profile: %s; audio_codec: %s; audio_sample_rate: %s; video_codec: %s; bframes: %s; reference_frames: %s; two_pass: %s; hls: %s; hls_segment: %s; audio_bitrate: %s; audio_only: %s; frame_rate: %s; h264_profile: %s; keyframe: %s; video_bitrate: %s; width: %s', $format, $audio_aac_profile, $audio_codec, $audio_sample_rate, $video_codec, $bframes, $reference_frames, $two_pass, $hls, $hls_segment, $audio_bitrate, $audio_only, $frame_rate, $h264_profile, $keyframe, $video_bitrate, $width), 'AWSEncodingController::getPresetId', __LINE__);
      // desired preset properties
      $jobPreset = array();
      $jobPreset['Container'] = $hls ? 'ts' : $format;
      
      // Audio
      $jobPreset['Audio'] = array();
      $jobPreset['Audio']['Codec'] = $audio_codec == 'aac' ? 'AAC' : $audio_codec;
      if ($audio_codec == 'aac' && $audio_aac_profile && $audio_aac_profile != 'auto') $jobPreset['Audio']['CodecOptions'] = array('Profile' => str_replace('V2', 'v2', strtoupper($audio_aac_profile)));
      $jobPreset['Audio']['SampleRate'] = $audio_sample_rate ? $audio_sample_rate : 'auto';
      $jobPreset['Audio']['BitRate'] = $audio_bitrate ? $audio_bitrate : self::AWS_DEFAULT_AUDIO_BITRATE;
      $jobPreset['Audio']['Channels'] = 'auto';
      
      // Video
      if (!$audio_only) {
        $jobPreset['Video'] = array();
        $jobPreset['Video']['Codec'] = $video_codec == 'vp8' ? 'vp8' : 'H.264';
        $jobPreset['Video']['CodecOptions'] = array();
        if ($format == 'mp4') {
          $jobPreset['Video']['CodecOptions']['Profile'] = $h264_profile ? $h264_profile : self::AWS_DEFAULT_H264_PROFILE;
          $jobPreset['Video']['CodecOptions']['Level'] = $this->getH264ProfileLevel($jobPreset['Video']['CodecOptions']['Profile']);
          if ($reference_frames) $jobPreset['Video']['CodecOptions']['MaxReferenceFrames'] = $reference_frames;
        }
        if ($keyframe) {
          $jobPreset['Video']['KeyframesMaxDist'] = $keyframe;
          $jobPreset['Video']['FixedGOP'] = TRUE;
        }
        $jobPreset['Video']['BitRate'] = $video_bitrate ? $video_bitrate : 'auto';
        $jobPreset['Video']['FrameRate'] = $frame_rate ? $frame_rate : 'auto';
        if ($width) {
          // The required value 'Video:MaxHeight' was not found.
          // The required value 'Thumbnails:MaxHeight' was not found.
          $jobPreset['Video']['MaxWidth'] = $width;
          $jobPreset['Video']['MaxHeight'] = 'auto';
          $jobPreset['Video']['SizingPolicy'] = self::AWS_SIZING_POLICY;
          $jobPreset['Video']['PaddingPolicy'] = self::AWS_PADDING_POLICY;
          $jobPreset['Video']['DisplayAspectRatio'] = 'auto';
          $jobPreset['Thumbnails'] = array('Format' => 'png', 'Interval' => self::AWS_THUMBNAILS_INTERVAL, 'MaxWidth' => $width, 'MaxHeight' => 'auto', 'SizingPolicy' => self::AWS_SIZING_POLICY, 'PaddingPolicy' => self::AWS_PADDING_POLICY);
        }
        else {
          $jobPreset['Video']['Resolution'] = 'auto';
          $jobPreset['Video']['AspectRatio'] = 'auto';
          $jobPreset['Thumbnails'] = array('Format' => 'png', 'Interval' => self::AWS_THUMBNAILS_INTERVAL, 'AspectRatio' => 'auto', 'Resolution' => self::AWS_THUMBNAIL_RESOLUTION);
        }
      }
      EncodingUtil::log(sprintf('Attempting to lookup or create preset: %s', $this->apiJsonEncode($jobPreset)), 'AWSEncodingController::getPresetId', __LINE__);
      
      // look for suitable existin preset
      foreach($this->aws_presets as $preset) {
        // compare to desired properties
        if ($this->comparePresets($jobPreset, $preset)) {
          EncodingUtil::log(sprintf('Existing preset %s/%s found matching desired output parameters', $preset['Id'], $preset['Name']), 'AWSEncodingController::getPresetId', __LINE__);
          $presetId = $preset['Id'];
        }
        else EncodingUtil::log(sprintf('Existing preset %s/%s does not match output parameters', $preset['Id'], $preset['Name']), 'AWSEncodingController::getPresetId', __LINE__);
      }
      // create new preset
      if (!$presetId) {
        $jobPreset['Name'] = 'Temporary Preset for Testing';
        $jobPreset['Description'] = 'This preset was created automatically by the CloudHarmony encoding test harness';
        EncodingUtil::log(sprintf('Suitable existing preset not found - attempting to create'), 'AWSEncodingController::getPresetId', __LINE__);
        if ($response = $this->invokeApi(self::AWS_PRESETS_RESOURCE, 'POST', NULL, $jobPreset)) {
          if (isset($response['Preset']['Id'])) {
            $presetId = $response['Preset']['Id'];
            EncodingUtil::log(sprintf('Create preset %s successfully', $presetId), 'AWSEncodingController::getPresetId', __LINE__);
            $this->aws_presets[$presetId] = $response['Preset'];
            // add ID of temporary preset to file used during cleanup to delete it
            exec(sprintf('echo "%s" >> %s', $presetId, $this->getPresetsFile()));
          }
          else EncodingUtil::log(sprintf('Create preset API POST successful - but did not include Response => Id value'), 'AWSEncodingController::getPresetId', __LINE__, TRUE);
        }
        else EncodingUtil::log(sprintf('Unable to invoke create preset API POST'), 'AWSEncodingController::getPresetId', __LINE__, TRUE);
      }
    }
    else EncodingUtil::log(sprintf('Unable to load existing presets'), 'AWSEncodingController::getPresetId', __LINE__, TRUE);
    
    return $presetId;
  }
  
  /**
   * invokes the get presets API request and returns the results. Returns NULL
   * on error
   * @param string $nextPageToken if being invoked for paginated results - this 
   * parameter may be specified
   * @return mixed
   */
  private final function getPresets($nextPageToken=NULL) {
    $presets = NULL;
    if ($response = $this->invokeApi(sprintf('%s%s', self::AWS_PRESETS_RESOURCE, $nextPageToken ? sprintf('/pageToken=%s', $nextPageToken) : ''))) {
      if (isset($response['Presets']) && is_array($response['Presets'])) {
        $presets = array();
        EncodingUtil::log(sprintf('Get presets API request successful - API response includes %d presets. NextPageToken: %s', count($response['Presets']), isset($response['NextPageToken']) ? $response['NextPageToken'] : NULL), 'AWSEncodingController::getPresets', __LINE__);
        foreach($response['Presets'] as $preset) {
          $presets[$preset['Id']] = $preset;
        }
        if (isset($response['NextPageToken']) && $response['NextPageToken'] && ($more = $this->getPresets($response['NextPageToken']))) {
          EncodingUtil::log(sprintf('Successfully invoked next page of getPresets using NextPageToken %s. %d presets returned', $response['NextPageToken'], count($more)), 'AWSEncodingController::getPresets', __LINE__);
          foreach(array_keys($more) as $presetId) {
            $presets[$presetId] = $more[$presetId];
          }
        }
        else EncodingUtil::log(sprintf('Unable to invoke next page of getPresets using NextPageToken %s', $response['NextPageToken']), 'AWSEncodingController::getPresets', __LINE__, TRUE);
      }
      else EncodingUtil::log(sprintf('Get presets API request successful - but failed to include "Presets" attribute in the response'), 'AWSEncodingController::getPresets', __LINE__, TRUE);
    }
    else EncodingUtil::log(sprintf('Unable to invoke get presets API request to retrieve account presets'), 'AWSEncodingController::getPresets', __LINE__, TRUE);
    
    return $presets;
  }
  
  /**
   * returns the API URL for the designated $url
   * @param string $uri the URI to return URL for
   * @param array $params optional hash of query parameters
   * @return string
   */
  private function getUrl($uri, $params=NULL) {
    $url = sprintf('https://%s%s%s', $this->aws_host, substr($uri, 0, 1) == '/' ? '' : '/', $uri);
    if (is_array($params)) {
      foreach(array_keys($params) as $i => $key) $url .= sprintf('%s%s=%s', ($i ? '&' : '?'), urlencode($key), urlencode($params[$key]));
    }
    EncodingUtil::log(sprintf('Created URL %s', $url), 'AWSEncodingController::getUrl', __LINE__);
    return $url;
  }
  
  /**
   * may be used to perform pre-usage initialization. return TRUE on success, 
   * FALSE on failure
   * @return boolean
   */
  protected function init() {
    $success = FALSE;
    $this->aws_pipeline = getenv('bm_param_service_param1') ? getenv('bm_param_service_param1') : self::AWS_PIPELINE_ALL;
    $this->aws_region = $this->service_region ? $this->service_region : self::DEFAULT_AWS_REGION;
    $this->aws_host = str_replace('{region}', $this->service_region, self::AWS_API_HOST);
    
    // attempt to identifier job pipelines
    if ($pipelines = $this->getPipelines()) {
      $this->aws_job_pipelines = array();
      foreach($pipelines as $id => $name) {
        if ($this->aws_pipeline == self::AWS_PIPELINE_ALL || $this->aws_pipeline == self::AWS_PIPELINE_FIRST || $id == $this->aws_pipeline || $name == $this->aws_pipeline) {
          $this->aws_job_pipelines[$id] = $name;
          EncodingUtil::log(sprintf('Added pipeline %s/%s matching pipeline runtime parameter %s', $id, $name, $this->aws_pipeline), 'AWSEncodingController::init', __LINE__);
          if ($this->aws_pipeline != self::AWS_PIPELINE_ALL) break;
        }
      }
      if ($success = count($this->aws_job_pipelines) ? TRUE : FALSE) {
        EncodingUtil::log(sprintf('Initialization successful - %d pipelines assigned and API host %s', count($this->aws_job_pipelines), $this->aws_host), 'AWSEncodingController::init', __LINE__);
      }
      else {
        $this->aws_job_pipelines = NULL;
        EncodingUtil::log(sprintf('Initialization failed - unable to find a matching pipeline for runtime pipeline parameter %s', $this->aws_pipeline), 'AWSEncodingController::init', __LINE__, TRUE);
      }
    }
    else EncodingUtil::log(sprintf('Unable to get account AWS transcoder pipelines'), 'AWSEncodingController::init', __LINE__, TRUE);
    
    return $success;
  }
  
  /**
   * return TRUE if the initial status of an encoding job is 'download', 
   * meaning the input must first be downloaded from the origin to the 
   * encoding service. If overridden and returns FALSE, it will be assumed that
   * the initial status is 'queue'
   * @return boolean
   */
  protected function initialStatusDownload() {
    return FALSE;
  }
  
  /**
   * invokes an API action and returns the response. Returns FALSE if the call 
   * results in a non 2xx status code. Returns an object if the response 
   * provides one in the body, otherwise returns TRUE. Returns NULL on error
   * @param string $uri the API URI to invoke
   * @param string $method the HTTP method to use - default is GET
   * @param array $params optional hash of query parameters
   * @param mixed $body optional object to include in the request body 
   * (json formatted) - for non PUT and POST requests only
   * @return mixed
   */
  private function invokeApi($uri, $method='GET', $params=NULL, $body=NULL) {
    $response = NULL;
    $request = array('method' => $method, 'url' => $this->getUrl($uri, $params), 'headers' => $this->getHeaders($uri, $method, $params, $body));
    if ($body && ($method == 'POST' || $method == 'PUT')) $request['body'] = $this->apiJsonEncode($body);
    if ($result = EncodingUtil::curl(array($request), TRUE)) {
      if ($result['status'][0] >= 200 && $result['status'][0] < 300) {
        EncodingUtil::log(sprintf('%s %s completed successfully with status code %d', $method, $uri, $result['status'][0]), 'AWSEncodingController::invokeApi', __LINE__);
        if (!$result['body'][0] || !($response = json_decode($result['body'][0], TRUE))) $response = TRUE;
      }
      else if ($result['status'][0] == 429) {
        EncodingUtil::log(sprintf('Got API rate throttling response code %d. Sleeping 1 second and retrying request', $result['status'][0]), 'AWSEncodingController::invokeApi', __LINE__, TRUE);
        sleep(1);
        return $this->invokeApi($uri, $method, $params, $body);
      }
      else {
        EncodingUtil::log(sprintf('%s %s resulting in status code %d, body: %s', $method, $uri, $result['status'][0], isset($result['body'][0]) ? $result['body'][0] : ''), 'AWSEncodingController::invokeApi', __LINE__, TRUE);
        $response = FALSE;
      }
    }
    else EncodingUtil::log(sprintf('%s %s resulting could not be invoked', $method, $uri), 'AWSEncodingController::invokeApi', __LINE__, TRUE);
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
      if ($response = $this->invokeApi(sprintf('%s/%s', self::AWS_JOBS_RESOURCE, $jobId))) {
        if (isset($response['Job']) && isset($response['Job']['Status']) && isset($response['Job']['Outputs']) && is_array($response['Job']['Outputs'])) {
          $stats['output_failed'] = 0;
          $stats['output_success'] = 0;
          foreach($response['Job']['Outputs'] as $i => $output) {
            if ($output['Status'] == 'Complete') {
              $stats['output_success']++;
              if (isset($output['Duration'])) {
                if (!isset($stats['output_durations'])) $stats['output_durations'] = array();
                $stats['output_durations'][] = $output['Duration']*1;
              }
              if (isset($output['Width']) && isset($output['Height'])) {
                if (!isset($stats['output_video_resolutions'])) $stats['output_video_resolutions'] = array();
                $stats['output_video_resolutions'][] = sprintf('%dx%d', $output['Width'], $output['Height']);
              }
            }
            else $stats['output_failed']++;
          }
          // convert array values to csv
          foreach(array_keys($stats) as $key) if (is_array($stats[$key])) $stats[$key] = implode(',', $stats[$key]);
        }
        else EncodingUtil::log(sprintf('Read job response for job %s did not include Job => Status or Job => Outputs', $jobId), 'AWSEncodingController::jobStats', __LINE__, TRUE);
      }
      else EncodingUtil::log(sprintf('Unable to invoke job status API request'), 'AWSEncodingController::jobStats', __LINE__, TRUE);
    }
    else EncodingUtil::log(sprintf('Invoked without specifying jobId'), 'AWSEncodingController::jobStats', __LINE__, TRUE);
    
    return count($stats) ? $stats : NULL;
  }
  
  /**
   * encodes an object (PHP array) into an AWS compatible JSON string. The 
   * AWS API uses strings for all values (i.e. numerical and boolean types are
   * not supported), so these values must first be stringified before json 
   * encoding
   * @param array $obj the object to encode
   * @return string
   */
  private function apiJsonEncode($obj) {
    $json = NULL;
    if (is_array($obj)) {
      $keys = array_keys($obj);
      $is_array = $keys[0] === 0;
      $json = $is_array ? '[' : '{';
      foreach($obj as $key => $val) {
        if (is_array($val)) $val = $this->apiJsonEncode($val);
        else if (is_bool($val)) $val = $val ? '"true"' : '"false"';
        else if (is_numeric($val)) $val = sprintf('"%s"', $val);
        else $val = json_encode($val);
        $json .= sprintf('%s%s%s', strlen($json) == 1 ? '' : ', ', $is_array ? '': '"' . $key . '": ', $val);
      }
      $json .= $is_array ? ']' : '}';
    }
    return $json;
  }
  
  /**
   * return TRUE if the designated storage service and region is the same as 
   * the encoding service
   * @param ObjectStorageController $storage_controller the storage controller
   * implementation for input and output files
   * @return boolean
   */
  protected function sameRegion(&$storage_controller) {
    return $storage_controller->getApi() == 's3' && $this->aws_region == $storage_controller->getApiRegion();
  }
  
  /**
   * returns an authorization signature for the parameters specified
   * @param array $headers the headers for the request
   * @param string $uri the request URI
   * @param string $method the http method
   * @param array $params optional hash of query parameters
   * @param mixed $body optional body for the API request. may be a string or
   * an array representing a form
   * @return string
   */
  private function sign($headers, $uri, $method, $params, $body) {
    
    $canonical_query_string = '';
    if (is_array($params)) {
      foreach($params as $key => $val) {
        unset($params[$key]);
        $params[urlencode($key)] = urlencode($val);
      }
      ksort($params);
      foreach($params as $key => $val) $canonical_query_string .= ($canonical_query_string ? '&' : '') . $key . '=' . $val;
    }
    
    $canonical_headers = '';
    $signed_headers = '';
    ksort($headers);
    foreach($headers as $key => $val) {
      $canonical_headers .= strtolower($key) . ':' . trim($val) . "\n";
      $signed_headers .= ($signed_headers ? ';' : '') . strtolower($key);
    }
    
    $request_payload = '';
    if (is_array($body)) $request_payload = $this->apiJsonEncode($body);
    else if ($body) $request_payload = $body;
    $request_payload = bin2hex(hash(self::AWS_HASH_ALGORITHM, $request_payload, TRUE));
    
    // uri must begin with '/'
    if (!$uri) $uri = '/';
    else if (substr($uri, 0, 1) != '/') $uri = '/' . $uri;
    
    $canonical_request = sprintf("%s\n%s\n%s\n%s\n%s\n%s", $method, $uri, $canonical_query_string, $canonical_headers, $signed_headers, $request_payload);
    $hashed_canonical_request = bin2hex(hash(self::AWS_HASH_ALGORITHM, $canonical_request, TRUE));
    EncodingUtil::log(sprintf('Created canonical request: %s; hashed: %s', str_replace("\n", '\n', $canonical_request), $hashed_canonical_request), 'AWSEncodingController::sign', __LINE__);
    $ymd = gmdate('Ymd');
    
    // generate hmac sequence for signing
    $string = sprintf("%s\n%s\n%s/%s/%s/%s\n%s", self::AWS_API_ALGORITHM, $headers[self::AWS_API_DATE_HEADER], $ymd, $this->service_region, self::AWS_API_SERVICE_ID, self::AWS_API_REQUEST_ID, $hashed_canonical_request);    
    $k_date = hash_hmac(self::AWS_HASH_ALGORITHM, $ymd, 'AWS4' . $this->service_secret, TRUE);
    $k_region = hash_hmac(self::AWS_HASH_ALGORITHM, $this->service_region, $k_date, TRUE);
    $k_service = hash_hmac(self::AWS_HASH_ALGORITHM, self::AWS_API_SERVICE_ID, $k_region, TRUE);
    $k_signing = hash_hmac(self::AWS_HASH_ALGORITHM, self::AWS_API_REQUEST_ID, $k_service, TRUE);
    EncodingUtil::log(sprintf('Signing string %s', str_replace("\n", '\n', $string)), 'AWSEncodingController::sign', __LINE__);
    
    // sign string
    $signature = bin2hex(hash_hmac(self::AWS_HASH_ALGORITHM, $string, $k_signing, TRUE));
    
    // build Authentication header
    $auth = sprintf('%s Credential=%s/%s/%s/%s/%s,SignedHeaders=%s,Signature=%s', self::AWS_API_ALGORITHM, $this->service_key, $ymd, $this->service_region, self::AWS_API_SERVICE_ID, self::AWS_API_REQUEST_ID, $signed_headers, $signature);
    EncodingUtil::log(sprintf('Returning authorization %s', $auth), 'AWSEncodingController::sign', __LINE__);
    return $auth;
  }
  
  /**
   * return TRUE if the service region $region is valid, FALSE otherwise
   * @param string $region the (encoding) region to validate (may be NULL if
   * use did not specify an explicit region)
   * @return boolean
   */
  protected function validateServiceRegion($region) {
    return in_array($region, explode(',', self::AWS_REGIONS));
  }
  
}
?>
