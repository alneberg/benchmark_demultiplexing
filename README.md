# README


## TODO
 - [x] Collect PID of the started run
 - [x] Create file when run is finished
 - [x] Only start a run if the previous is done
 - [x] Check if the run has already been done in previous script run
 - [x] Output psutil stats into a text file with timestamps (to avoid overwriting)
 - [x] Output psutil stats for the specific pid as well as for the entire system
 - [ ] Collect both memory and cpu stats
 - [x] Handle parallel runs
 - [ ] Check for more stats (IO)
 - [ ] process specific memory usage
 - [x] Delete output files after run
 - [ ] Modify the command to fit each clone/run

- [x]] Cleanup fails when directory is not empty
- [ ] process cpu percentage is always 0...


Runs will create the same output directory, and delete if once the command is done.
