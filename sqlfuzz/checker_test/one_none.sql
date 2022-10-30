-- 10040

select distinct 
  case when subq_0.c2 is not NULL then subq_0.c4 else subq_0.c4 end
     as c0, 
  subq_0.c3 as c1, 
  subq_0.c0 as c2, 
  subq_0.c7 as c3, 
  subq_0.c4 as c4, 
  subq_0.c5 as c5, 
  subq_0.c0 as c6, 
  subq_0.c0 as c7, 
  subq_0.c6 as c8, 
  subq_0.c7 as c9, 
  subq_0.c2 as c10
from 
  (select  
        cast(coalesce(ref_0.O_W_ID,
          ref_0.O_W_ID) as SMALLINT) as c0, 
        ref_0.O_D_ID as c1, 
        ref_0.O_W_ID as c2, 
        40 as c3, 
        ref_0.O_W_ID as c4, 
        52 as c5, 
        10 as c6, 
        ref_0.O_C_ID as c7
      from 
        main.ORDERS as ref_0
      where ref_0.O_OL_CNT is not NULL) as subq_0
where subq_0.c0 is not NULL;

