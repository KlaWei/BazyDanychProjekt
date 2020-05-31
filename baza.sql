create role app with login password 'qwerty';

drop table if exists employee;

create table if not exists employee (
   id serial primary key not null,
   supervisor_id integer references employee(id),
   password text,
   data text,
   time_in integer,
   time_out integer
);

grant select, insert, update, delete on employee to app;
