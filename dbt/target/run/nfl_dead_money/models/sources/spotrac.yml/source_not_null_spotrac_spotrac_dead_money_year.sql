
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select year
from "nfl_dead_money"."main"."spotrac_dead_money"
where year is null



  
  
      
    ) dbt_internal_test