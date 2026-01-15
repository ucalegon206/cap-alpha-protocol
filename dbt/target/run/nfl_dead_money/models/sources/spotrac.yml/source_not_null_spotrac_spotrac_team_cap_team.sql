
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select team
from "nfl_dead_money"."main"."spotrac_team_cap"
where team is null



  
  
      
    ) dbt_internal_test