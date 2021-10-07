create or replace function public.tasks_per_user(p_user_id int8,p_project_ids varchar,p_start_date varchar,p_end_date varchar,p_task_type varchar)
   returns int4
   language plpgsql
  as
$$
declare 
v_total int4;
begin
select total into v_total
from dblink('dbname=tm port=5432 host=TASKING_MANAGER_DB_HOST user=TASKING_MANAGER_DB_USER password=TASKING_MANAGER_DB_PASSWORD','
				select count(task_id) 
				from public.task_history
				where action_text = ''' || p_task_type ||'''
				and action_date between ''' || p_start_date ||''' and ''' || p_end_date ||'''
				and user_id = ' || p_user_id ||'
				and project_id in (' || p_project_ids ||')
				 ' 
) as t1 (total int4);

return v_total;

exception when others then
raise notice 'error % %', SQLERRM, SQLSTATE;
return -1;
end; 
$$;


create or replace function public.editors_per_user(p_user_id int8,p_start_date varchar,p_end_date varchar)
   returns varchar
   language plpgsql
  as
$$
declare 
v_editors varchar = '';
r osm_changeset%rowtype;
temprow record;
begin
	FOR temprow in select distinct (tags-> 'created_by') editor from public.osm_changeset c where c.created_at > p_start_date::timestamp and c.created_at < p_end_date::timestamp and user_id = p_user_id
    LOOP
        
    raise notice 'Editor %',temprow.editor ;
    	v_editors = v_editors || temprow.editor || ',';
		
    END LOOP;   	

return v_editors;

exception when others then
raise notice 'error % %', SQLERRM, SQLSTATE;
return sqlerrm;
end; 
$$;

