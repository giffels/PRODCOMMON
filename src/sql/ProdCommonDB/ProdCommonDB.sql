
/*
 * Create the ProdMgr database
 */
/*
 * This should be the default setting for InnoDB tables;
 */
SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ;
/*
 * Do not commit after every transaction
 */
SET AUTOCOMMIT = 0;


CREATE TABLE ws_last_call_server
  (
    client_id             varchar(150)    not null,
    service_call          varchar(255)   not null,
    service_parameters    mediumtext     not null,
    service_result        longtext     not null,
    log_time timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
   
    unique(client_id,service_call)

  ) Type=InnoDB;

CREATE TABLE ws_last_call_client
  (
    component_id          varchar(150)    not null,
    service_call          varchar(255)   not null,
    server_url            varchar(255)   not null,
    service_parameters    mediumtext     not null,
    call_state            ENUM('call_placed','result_retrieved'),
    log_time timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,


    unique(component_id,service_call,server_url)

  ) Type=InnoDB;

