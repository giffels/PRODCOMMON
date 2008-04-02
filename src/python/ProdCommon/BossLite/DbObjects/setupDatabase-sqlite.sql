
CREATE TABLE bl_task
  (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255),
    start_dir TEXT,
    output_dir TEXT,
    global_sanbox TEXT,
    cfg_name TEXT,
    server_name TEXT,
    job_type TEXT,
    script_name TEXT,
    user_proxy TEXT,
    unique(name)
  );

CREATE TABLE bl_job
  (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INT NOT NULL,
    job_id INT NOT NULL,
    name VARCHAR(255),
    executable TEXT,
    arguments TEXT,
    stdin TEXT,
    stdout TEXT,
    stderr TEXT,
    log_file TEXT,
    input_files TEXT,
    output_files TEXT,
    file_block TEXT,
    dls_destination TEXT,
    submission_number INT default 0,
    UNIQUE(job_id, task_id),
    FOREIGN KEY(task_id) references bl_task(id) ON DELETE CASCADE
  );

CREATE TABLE bl_runningjob
  (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    submission INT NOT NULL,
    task_id INT NOT NULL,
    job_id INT NOT NULL,
    submission_path TEXT,
    scheduler TEXT,
    service TEXT,
    scheduler_id VARCHAR(255),
    scheduler_parent_id VARCHAR(255),
    status_scheduler VARCHAR(255),
    status VARCHAR(255),
    status_reason TEXT,
    status_history TEXT,
    destination TEXT,
    lb_timestamp TIMESTAMP,
    submission_time TIMESTAMP,
    start_time TIMESTAMP,
    stop_time TIMESTAMP,
    getoutput_time TIMESTAMP,
    execution_host TEXT,
    execution_path TEXT,
    execution_user VARCHAR(255),
    application_return_code INT,
    wrapper_return_code INT,
    sched_attr TEXT,
    process_status TEXT default 'not_handled',
    closed CHAR default "N",
    UNIQUE(submission, job_id, task_id),
    FOREIGN KEY(job_id) references bl_job(id) ON DELETE CASCADE,
    FOREIGN KEY(task_id) references bl_task(id) ON DELETE CASCADE
  );

