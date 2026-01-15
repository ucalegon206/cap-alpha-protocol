
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dead_money_millions
from "nfl_dead_money"."main"."spotrac_team_cap"
where dead_money_millions is null



  
  
      
    ) dbt_internal_test