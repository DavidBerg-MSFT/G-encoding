#!/usr/bin/php -q
<?php
/**
 * this script cleans up files created during testing
 *   0 cleanup successful
 *   1 cleanup failed
 */
require_once(dirname(__FILE__) . '/lib/encoding/EncodingController.php');
$status = 0;

if ($controller =& EncodingController::getInstance()) {
  // sleep a few seconds
  sleep(5);
  if ($controller->cleanup()) EncodingUtil::log(sprintf('Cleanup successful'), 'run-cleanup.php', __LINE__);
  else {
    $status = 1;
    EncodingUtil::log(sprintf('Cleanup failed'), 'run-cleanup.php', __LINE__, TRUE);
  }
}

exit($status);
?>