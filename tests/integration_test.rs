use jobcannond::jobs::Job;
use std::time;

enum Action {
    Send(String),
    Receive, // function that takes string and returns Result
    Wait(time::Duration),
    Sync, // Not sure what object, some semaphore perhaps?
}

#[test]
fn test_put() {
}

fn run_scenario(scenario: Vec<Vec<Action>>) {
    // spawn separate thread for each scenario
    //
}
