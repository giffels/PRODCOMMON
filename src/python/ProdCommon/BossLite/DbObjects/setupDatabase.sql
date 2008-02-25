
SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SET AUTOCOMMIT = 0;

CREATE TABLE bl_task
  (
    id INT auto_increment,
    name VARCHAR(255),
    start_dir TEXT,
    output_dir TEXT,
    global_sanbox TEXT,
    cfg_name TEXT,
    server_name TEXT,
    job_type TEXT,
    script_name TEXT,
    primary key(id),
    unique(name)
  )
  TYPE = InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE bl_job
  (
    id INT auto_increment,
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
    PRIMARY KEY(id),
    UNIQUE(job_id, task_id),
    FOREIGN KEY(task_id) references bl_task(id) ON DELETE CASCADE
  )
  TYPE = InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE bl_runningjob
  (
    id INT auto_increment,
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
    closed CHAR default "N",
    PRIMARY KEY(id),
    UNIQUE(submission, job_id, task_id),
    FOREIGN KEY(job_id) references bl_job(job_id) ON DELETE CASCADE,
    FOREIGN KEY(task_id) references bl_task(id) ON DELETE CASCADE
  )
  TYPE = InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE jt_group 
  (
  id int(11) NOT NULL auto_increment,
  group_id int(11) default NULL,
  task_id int(11) NOT NULL,
  job_id int(11) NOT NULL,
  PRIMARY KEY  (id),
  UNIQUE KEY task_id (task_id,job_id),
  KEY job_id (job_id)
  ) 
  TYPE = InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE jt_activejobs 
  (
  job_id varchar(255) NOT NULL,
  status enum('output_not_requested','output_requested','in_progress',
       'output_retrieved','output_processed') default 'output_not_requested',
  directory text,
  output text,
  boss_status varchar(10) default '',
  job_spec_id varchar(255) default NULL,
  UNIQUE KEY job_id (job_id)
  ) 
  TYPE = InnoDB DEFAULT CHARSET=latin1;

