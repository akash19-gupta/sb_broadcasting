view: overall_mis {
  derived_table: {
    sql: With MTD as
(Select Flag,nsm_name,regional_manager_name,BC_device_flag,final_device_type,replacement_flag,state,city,region,refurb_flag,
Case when total_txn<5 then '0 to 5'
when total_txn between 5  and 50 then '5 to 50'
when total_txn between 50  and 100 then '50 to 100'
when total_txn between 100  and 200 then '100 to 200'
when total_txn between 201  and 500 then '200 to 500'
when total_txn>500 then '500+' end as  txn_bucket,
case when ratio>90 then '>90'

      when ratio between 80 and 90 then '80 to 90'

      when ratio between 70 and 80 then '70 to 80'

            when ratio between 50 and 70 then '50 to 70'

            when ratio between 30 and 50 then '30 to 50'

            when ratio between 1 and 30 then '<30'

            when ratio=0 then '0' else 'na' end as
            txn_bc_percent,
count(distinct paytm_merchant_id) as Total_devices,
sum(bc_txn_in_0_1_sec)bc_txn_in_0_1_sec,
sum(bc_txn_in_2_5_sec)bc_txn_in_2_5_sec,
sum(bc_txn_6_10_sec)bc_txn_6_10_sec,
sum(bc_txn_in_11_30_sec)bc_txn_in_11_30_sec,
sum(bc_txn_in_31_60_sec)bc_txn_in_31_60_sec,
sum(bc_txn_above_60_sec)bc_txn_above_60_sec,
sum(total_txn) as total_txn,sum(total_bc_txn) total_bc_txn from
(SELECT 'MTD' as Flag,COALESCE(nsm_name,'others')nsm_name
,COALESCE(regional_manager_name,'others')regional_manager_name,paytm_merchant_id,iot_device_id,final_device_type,replacement_flag,COALESCE(state,'others')state
,COALESCE(city,'others')city,COALESCE(region,'others')region,refurb_flag,
case when sum(bc_txn)>0 then 1 else 0 end as BC_device_flag,
 sum(txn) total_txn,sum(bc_txn) total_bc_txn,
round(cast(sum(bc_txn) as double )*100/cast(sum(txn) as double),0) as ratio,
SUM(COALESCE(_0sec, 0)) + SUM(COALESCE(_1sec, 0)) as bc_txn_in_0_1_sec,
SUM(COALESCE(_2sec, 0)) + SUM(COALESCE(_3sec, 0)) + SUM(COALESCE(_4sec, 0)) + SUM(COALESCE(_5sec, 0)) as bc_txn_in_2_5_sec,
SUM(COALESCE(_6sec, 0)) + SUM(COALESCE(_7sec, 0)) + SUM(COALESCE(_8sec, 0)) + SUM(COALESCE(_9sec, 0)) + SUM(COALESCE(_10sec, 0))  as bc_txn_6_10_sec,
SUM(COALESCE(_11sec, 0)) + SUM(COALESCE(_12sec, 0)) + SUM(COALESCE(_13sec, 0)) + SUM(COALESCE(_14sec, 0))+
SUM(COALESCE(_15sec, 0))+SUM(COALESCE(_16sec, 0))+SUM(COALESCE(_17sec, 0))+SUM(COALESCE(_18sec, 0))+
SUM(COALESCE(_19sec, 0))+SUM(COALESCE(_20sec, 0))+SUM(COALESCE(_20_30_sec, 0)) as bc_txn_in_11_30_sec,
SUM(COALESCE(_31_60_sec, 0)) as bc_txn_in_31_60_sec,
SUM(COALESCE(_61_90_sec, 0)) + SUM(COALESCE(_91_120_sec, 0)) + SUM(COALESCE(_121_180_sec, 0)) +
SUM(COALESCE(_181_240_sec, 0)) + SUM(COALESCE(above_240sec, 0)) as bc_txn_above_60_sec
from user_paytm_payments.sb_bc_base_snapshot where merchant_txn_flag = 'Active'
and date(txn_date) >= DATE_TRUNC('month', date(current_date-interval'1'day))
and date(txn_date) <= date(current_date-interval'2'day)
and final_device_type is not null
group by 1,2,3,4,5,6,7,8,9,10,11
) group by 1,2,3,4,5,6,7,8,9,10,11,12),

lmtd as
(Select Flag,nsm_name,regional_manager_name,BC_device_flag,final_device_type,replacement_flag,state,city,region,refurb_flag,
Case when total_txn<5 then '0 to 5'
when total_txn between 5  and 50 then '5 to 50'
when total_txn between 50  and 100 then '50 to 100'
when total_txn between 100  and 200 then '100 to 200'
when total_txn between 201  and 500 then '200 to 500'
when total_txn>500 then '500+' end as  txn_bucket,
case when ratio>90 then '>90'

      when ratio between 80 and 90 then '80 to 90'

      when ratio between 70 and 80 then '70 to 80'

            when ratio between 50 and 70 then '50 to 70'

            when ratio between 30 and 50 then '30 to 50'

            when ratio between 1 and 30 then '<30'

            when ratio=0 then '0' else 'na' end as
            txn_bc_percent,
count(distinct paytm_merchant_id) as Total_devices,
sum(bc_txn_in_0_1_sec)bc_txn_in_0_1_sec,
sum(bc_txn_in_2_5_sec)bc_txn_in_2_5_sec,
sum(bc_txn_6_10_sec)bc_txn_6_10_sec,
sum(bc_txn_in_11_30_sec)bc_txn_in_11_30_sec,
sum(bc_txn_in_31_60_sec)bc_txn_in_31_60_sec,
sum(bc_txn_above_60_sec)bc_txn_above_60_sec,
sum(total_txn) as total_txn,sum(total_bc_txn) total_bc_txn from
(SELECT 'LMTD' as Flag,COALESCE(nsm_name,'others')nsm_name
,COALESCE(regional_manager_name,'others')regional_manager_name,paytm_merchant_id,iot_device_id,final_device_type,replacement_flag,COALESCE(state,'others')state
,COALESCE(city,'others')city,COALESCE(region,'others')region,refurb_flag,
case when sum(bc_txn)>0 then 1 else 0 end as BC_device_flag,
 sum(txn) total_txn,sum(bc_txn) total_bc_txn,
round(cast(sum(bc_txn) as double )*100/cast(sum(txn) as double),0) as ratio,
SUM(COALESCE(_0sec, 0)) + SUM(COALESCE(_1sec, 0)) as bc_txn_in_0_1_sec,
SUM(COALESCE(_2sec, 0)) + SUM(COALESCE(_3sec, 0)) + SUM(COALESCE(_4sec, 0)) + SUM(COALESCE(_5sec, 0)) as bc_txn_in_2_5_sec,
SUM(COALESCE(_6sec, 0)) + SUM(COALESCE(_7sec, 0)) + SUM(COALESCE(_8sec, 0)) + SUM(COALESCE(_9sec, 0)) + SUM(COALESCE(_10sec, 0))  as bc_txn_6_10_sec,
SUM(COALESCE(_11sec, 0)) + SUM(COALESCE(_12sec, 0)) + SUM(COALESCE(_13sec, 0)) + SUM(COALESCE(_14sec, 0))+
SUM(COALESCE(_15sec, 0))+SUM(COALESCE(_16sec, 0))+SUM(COALESCE(_17sec, 0))+SUM(COALESCE(_18sec, 0))+
SUM(COALESCE(_19sec, 0))+SUM(COALESCE(_20sec, 0))+SUM(COALESCE(_20_30_sec, 0)) as bc_txn_in_11_30_sec,
SUM(COALESCE(_31_60_sec, 0)) as bc_txn_in_31_60_sec,
SUM(COALESCE(_61_90_sec, 0)) + SUM(COALESCE(_91_120_sec, 0)) + SUM(COALESCE(_121_180_sec, 0)) +
SUM(COALESCE(_181_240_sec, 0)) + SUM(COALESCE(above_240sec, 0)) as bc_txn_above_60_sec
from user_paytm_payments.sb_bc_base_snapshot where merchant_txn_flag = 'Active'
and date(txn_date) <= DATE_TRUNC('day', (current_date-interval'2'day) - INTERVAL '1' month)
and date(txn_date) >= DATE_TRUNC('month', (current_date-interval'1'day) - INTERVAL '1' month)
and final_device_type is not null
group by 1,2,3,4,5,6,7,8,9,10,11
) group by 1,2,3,4,5,6,7,8,9,10,11,12)


(
Select * from MTD

union all select * from LMTD)
      ;;
  }

  suggestions: yes

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: flag {
    type: string
    sql: ${TABLE}.Flag ;;
  }

  dimension: replacement_flag {
    type: number
    sql: ${TABLE}.replacement_flag ;;
  }

  dimension: nsm_name {
    type: string
    sql: ${TABLE}.nsm_name ;;
  }

  dimension: regional_manager_name {
    type: string
    sql: ${TABLE}.regional_manager_name ;;
  }


  dimension: bc_device_flag {
    type: number
    sql: ${TABLE}.BC_device_flag ;;
  }

  dimension: refurb_flag {
    type: number
    sql: ${TABLE}.refurb_flag ;;
  }

  dimension: city {
    type: string
    sql: ${TABLE}.city ;;
  }

  dimension: state {
    type: string
    sql: ${TABLE}.state ;;
  }

  dimension: region {
    type: string
    sql: ${TABLE}.region ;;
  }

  dimension: final_device_type {
    type: string
    sql: ${TABLE}.final_device_type ;;
  }

  dimension: txn_bucket {
    type: number
    sql: ${TABLE}.txn_bucket ;;
  }

  dimension: txn_bc_percent {
    type: string
    sql: ${TABLE}.txn_bc_percent ;;
  }

  dimension: total_devices {
    type: number
    sql: ${TABLE}.Total_devices ;;
  }

  dimension: total_txn {
    type: number
    sql: ${TABLE}.total_txn ;;
  }

  dimension: total_bc_txn {
    type: number
    sql: ${TABLE}.total_bc_txn ;;
  }

  set: detail {
    fields: [
      flag,
      replacement_flag,
      nsm_name,
      regional_manager_name,
      bc_device_flag,
      refurb_flag,
      city,
      state,
      region,
      final_device_type,
      txn_bucket,
      txn_bc_percent,
      total_devices,
      total_txn,
      total_bc_txn
    ]
  }
}


explore: overall_mis{}
