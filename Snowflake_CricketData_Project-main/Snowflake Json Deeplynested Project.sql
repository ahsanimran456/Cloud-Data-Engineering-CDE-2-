use role sysadmin
use warehouse compute_wh


create database if not exists cricket;
create or replace schema cricket.land;
create or replace schema cricket.raw;
create or replace schema cricket.clean;
create or replace schema cricket.consumption;


use schema cricket.land

create or replace file format cricket.land.json_format
type = 'json'
null_if=('\\n','null','')
strip_outer_array= true --- it will treat every line as a seperate row
comment ='json outer strip true';

show file formats like '%j%'
drop file format if exists CRICKET_LAND_JSON_FORMAT;


create or replace stage cricket.land.mystg

list @mystg

list @mystg/cricket/json
select $1
from mystg/cricket/json/1384401.json.gz (file_format => 'json_format');



select 
        cricketdata.$1:meta::variant as meta, 
        cricketdata.$1:info::variant as info, 
        cricketdata.$1:innings::array as innings, 
        metadata$filename as file_name,
        metadata$file_row_number int,
        metadata$file_content_key text,
        metadata$file_last_modified stg_modified_ts,
     from @mystg/cricket/json/1384408.json(file_format => 'json_format') cricketdata;




     use schema raw
     create or replace transient table matchraw_table(
     meta object not null,
     info  variant not null,
     innings array not null,
     stg_file_name text not null,
     stg_file_row_number int not null,
     stg_file_hashkey text not null
     
     );
create or replace file format cricket.raw.json_format
type = 'json'
null_if=('\\n','null','')
strip_outer_array= true --- it will treat every line as a seperate row
comment ='json outer strip true';


copy into matchraw_table
from 
(
select 
        cricketdata.$1:meta::variant as meta, 
        cricketdata.$1:info::variant as info, 
        cricketdata.$1:innings::array as innings, 
        metadata$filename as file_name,
        metadata$file_row_number int,
        metadata$file_content_key text 
     from @cricket.land.mystg/cricket/json(file_format => 'json_format') cricketdata)
     on_error= continue;




select * from matchraw_table;


