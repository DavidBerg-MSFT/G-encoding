#!/usr/bin/php -q
<?php
/**
 * this script performs a test iteration. It utilizes the following exit status 
 * codes
 *   0 Iteration successful
 *   1 Iteration failed
 */
require_once(dirname(__FILE__) . '/lib/encoding/EncodingController.php');
$status = 0;

if ($controller =& EncodingController::getInstance()) {
  if ($controller->start()) {
    EncodingUtil::log(sprintf('Encoding jobs started successfully - polling for completion'), 'run.php', __LINE__);
    while(!$controller->poll()) sleep(1);
    EncodingUtil::log(sprintf('Encoding jobs are complete - getting output sizes'), 'run.php', __LINE__);
    $controller->setOutputSizes();
    // print results
  	print("\n\n[results]\n");
    $controller->stats();
  }
  else EncodingUtil::log(sprintf('Unable to perform encoding test'), 'run.php', __LINE__, TRUE);
}

exit($status);
?>