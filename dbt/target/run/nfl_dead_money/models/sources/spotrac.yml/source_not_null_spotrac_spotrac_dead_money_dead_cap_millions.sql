
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dead_cap_millions
from "nfl_dead_money"."main"."spotrac_dead_money"
where dead_cap_millions is null



  
  
      
    ) dbt_internal_test