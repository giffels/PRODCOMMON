
CREATE TABLE bl_task
  (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255),
    dataset VARCHAR(255),
    start_dir TEXT,
    output_dir TEXT,
    global_sanbox TEXT,
    cfg_name TEXT,
    server_name TEXT,
    job_type TEXT,
    total_events INT,
    user_proxy TEXT,
    outfile_basename TEXT,
    common_requirements TEXT,
    unique(name)
  );

CREATE TABLE bl_job
  (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INT NOT NULL,
    job_id INT NOT NULL,
    wmbsJob_id INT ,
    name VARCHAR(255),
    executable TEXT,
    events INT,
    arguments TEXT,
    stdin TEXT,
    stdout TEXT,
    stderr TEXT,
    input_files TEXT,
    output_files TEXT,
    dls_destination TEXT,
    submission_number INT default 0,
    closed CHAR default "N",
    UNIQUE(job_id, task_id),
    FOREIGN KEY(task_id) references bl_task(id) ON DELETE CASCADE
  );

CREATE TABLE bl_runningjob
  (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INT NOT NULL,
    task_id INT NOT NULL,
    submission INT NOT NULL,
    state VARCHAR(255),
    scheduler TEXT,
    service TEXT,
    sched_attr TEXT,
    scheduler_id VARCHAR(255),
    scheduler_parent_id VARCHAR(255),
    status_scheduler VARCHAR(255),
    status VARCHAR(255),
    status_reason TEXT,
    destination TEXT, 
    creation_timestamp TIMESTAMP,
    lb_timestamp TIMESTAMP,
    submission_time TIMESTAMP,
    scheduled_at_site TIMESTAMP,
    start_time TIMESTAMP,
    stop_time TIMESTAMP,
    stageout_time TIMESTAMP,
    getoutput_time TIMESTAMP,
    output_request_time TIMESTAMP,
    output_enqueue_time TIMESTAMP,
    getoutput_retry INT,
    output_dir TEXT,
    storage TEXT,
    lfn TEXT,
    application_return_code INT,
    wrapper_return_code INT,
    process_status TEXT default 'created',
    closed CHAR default "N",
    UNIQUE(submission, job_id, task_id),
    FOREIGN KEY(job_id) references bl_job(id) ON DELETE CASCADE,
    FOREIGN KEY(task_id) references bl_task(id) ON DELETE CASCADE
  );

