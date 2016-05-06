<?php
// Copyright 2004-present Facebook. All Rights Reserved.

final class FacebookDiffCreatedListener extends PhutilEventListener {

  public function register() {
    $this->listen(ArcanistEventType::TYPE_DIFF_WASCREATED);
  }

  public function handleEvent(PhutilEvent $event) {
    if ($event->getValue('unitResult') == ArcanistUnitWorkflow::RESULT_SKIP) {
      return;
    }

    $diffID = $event->getValue('diffID');

    $build_step = array(
      'name' => 'Build Cassandra',
      'shell' => './scripts/build',
    );

    $test_step = array(
      'name' => 'Test Cassandra',
      'shell' => './scripts/test --all',
    );

    $cmd_args = array(
      'name' => 'Istagram Cassandra Test',
      'oncall' => 'instagram',
      'steps' => array(
        $build_step,
        $test_step,
      ),
    );

    $job = array(
      'command' => 'SandcastleUniversalCommand',
      'command-args' => $cmd_args,
      'vcs' => 'ig-cassandra-git',
      'diff' => $diffID,
      'type' => 'lego',
      'alias' => 'ig-cassandra-test',
      );

    $sandcastle = new ArcanistSandcastleClient($event->getValue('workflow'));
    $sandcastle->createBundle();
    $sandcastle->enqueue($job);
  }

}
