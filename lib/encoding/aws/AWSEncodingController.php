<?php
/**
 * Implements testing functionality for the Zencoder platform
 */
class AWSEncodingController extends EncodingController {
  
  // default service region
  const DEFAULT_AWS_REGION = 'us-east-1';
  // base API URL
  const AWS_API_URL = 'https://elastictranscoder.{region}.amazonaws.com';
  // valid region identifiers
  const AWS_REGIONS = 'us-east-1,us-west-1,us-west-2,eu-west-1,ap-southeast-1,ap-southeast-2,ap-northeast-1,sa-east-1';
  
  // encoding.com region
  private $aws_region;
  
  /**
   * invoked once during validation. Should return TRUE if authentication is 
   * successful, FALSE otherwise. this method may reference the optional 
   * attributes $service_key, $service_secret and $service_region as necessary 
   * to complete authentication
   * @return boolean
   */
  protected function authenticate() {
    $authenticated = FALSE;
    // TODO
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
   * @param string $format desired output format. One of: aac, mp4, ogg, webm
   * @param string $audio_codec desired audio codec. One of: aac, vorbis
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
  protected function encode(&$storage_controller, $input, $input_format, $input_size, $format, $audio_codec, $video_codec, $bframes, $reference_frames, $two_pass, $hls, $hls_segment, $outputs) {
    $jobId = NULL;

    // TODO
    
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
   * return NULL on error (test will abort)
   * @return array
   */
  protected function getJobStatus($jobIds) {
    $status = array();
    if (is_array($jobIds) && count($jobIds)) {
      // TODO
    }
    else EncodingUtil::log(sprintf('Invoked without specifying any jobIds'), 'AWSEncodingController::getJobStatus', __LINE__, TRUE);
    
    return count($status) ? $status : NULL;
  }
  
  /**
   * may be used to perform pre-usage initialization. return TRUE on success, 
   * FALSE on failure
   * @return boolean
   */
  protected function init() {
    $this->aws_region = $this->service_region ? $this->service_region : self::DEFAULT_AWS_REGION;
    return TRUE;
  }
  
  /**
   * may be overridden to provide stats about a job input. These will be 
   * included in the test output if provided. The return value is a hash. The 
   * following stats may be returned:
   *   audio_bit_rate           input audio bit rate (kbps)
   *   audio_channels           input audio channels
   *   audio_codec              input audio codec
   *   duration                 duration (seconds - decimal) of the media file
   *   error                    optional error message(s)
   *   job_start                start time for the job as reported by the service 
   *                            (optional)
   *   job_stop                 stop time for the job as reported by the service 
   *                            (optional)
   *   job_time                 the total time for the job as reported by the service
   *   output_audio_bit_rate    output audio bit rates (csv) - reported by encoding 
   *                            service (optional)
   *   output_audio_channels    output audio channels (csv) - reported by encoding 
   *                            service (optional)
   *   output_audio_codecs      output audio codecs (csv) - reported by encoding 
   *                            service (optional)
   *   output_durations         Output durations (csv) - reported by encoding service 
   *                            (optional)
   *   output_failed            Number of outputs that failed to generate
   *   output_formats           Output formats (csv) - reported by encoding service 
   *                            (optional)
   *   output_success           Number of successful outputs generated
   *   output_total_bit_rates   Output total bit rates - kbps (csv) - as reported by 
    *                           encoding service (optional)
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
      // TODO
    }
    else EncodingUtil::log(sprintf('Invoked without specifying jobId'), 'AWSEncodingController::jobStats', __LINE__, TRUE);
    
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
    return $storage_controller->getApi() == 's3' && $this->aws_region == $storage_controller->getApiRegion();
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
