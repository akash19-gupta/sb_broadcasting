view: overall_mis {
  derived_table: {
    sql: with MTD as
      ( Select Flag,nsm_name,regional_manager_name,BC_device_flag,city,state,region,final_device_type,
      Case when total_txn<5 then 'below 5'
      when total_txn between 5  and 50 then '5 to 50 txn'
      when total_txn between 50  and 100 then '50 to 100 txn'
      when total_txn between 100  and 200 then '100 to 200 txn'
      when total_txn between 201  and 500 then '200 to 500 txn'
      when total_txn>500 then 'above 500 txn' end as  txn_bucket,
      case when ratio>90 then 'above 90_%'

      when ratio between 80 and 90 then '80 to 90'

      when ratio between 70 and 80 then '70 to 80'

      when ratio between 50 and 70 then '50 to 70'

      when ratio between 30 and 50 then '30 to 50'

      when ratio between 1 and 30 then 'below 30'

      when ratio=0 then '0' else 'na' end as
      txn_bc_percent,
      count(distinct paytm_merchant_id) as Total_devices,
      sum(total_txn) as total_txn,sum(total_bc_txn) total_bc_txn from (
      SELECT 'MTD' as Flag,nsm_name
      ,regional_manager_name,paytm_merchant_id,iot_device_id,city,state,region,final_device_type,case when sum(bc_txn)>0 then 1 else 0 end as BC_device_flag,
      sum(txn) total_txn,sum(bc_txn) total_bc_txn,
      round(cast(sum(bc_txn) as double )*100/cast(sum(txn) as double),0) as ratio

      from user_paytm_payments.sb_bc_base_snapshot where merchant_txn_flag = 'Active'
      and date(txn_date) >= DATE_TRUNC('month', date(current_date-interval'1'day))
      and date(txn_date) <= date(current_date-interval'1'day)
      group by 1,2,3,4,5,6,7,8,9
      ) group by 1,2,3,4,5,6,7,8,9,10),

      lMTD as
      (Select Flag,nsm_name,regional_manager_name,BC_device_flag,city,state,region,final_device_type,
      Case when total_txn<5 then 'below 5'
      when total_txn between 5  and 50 then '5 to 50 txn'
      when total_txn between 50  and 100 then '50 to 100 txn'
      when total_txn between 100  and 200 then '100 to 200 txn'
      when total_txn between 201  and 500 then '200 to 500 txn'
      when total_txn>500 then 'above 500 txn' end as  txn_bucket,
      case when ratio>90 then 'above 90_%'

      when ratio between 80 and 90 then '80 to 90'

      when ratio between 70 and 80 then '70 to 80'


      when ratio between 50 and 70 then '50 to 70'

      when ratio between 30 and 50 then '30 to 50'

      when ratio between 1 and 30 then 'below 30'

      when ratio=0 then '0' else 'na' end as
      txn_bc_percent,
      count(distinct paytm_merchant_id) as Total_devices,
      sum(total_txn) as total_txn,sum(total_bc_txn) total_bc_txn from (
      SELECT 'LMTD' as Flag,nsm_name
      ,regional_manager_name,paytm_merchant_id,iot_device_id,city,state,region,final_device_type,case when sum(bc_txn)>0 then 1 else 0 end as BC_device_flag,
      sum(txn) total_txn,sum(bc_txn) total_bc_txn,
      round(cast(sum(bc_txn) as double )*100/cast(sum(txn) as double),0) as ratio

      from user_paytm_payments.sb_bc_base_snapshot where merchant_txn_flag = 'Active'
      and date(txn_date) <= DATE_TRUNC('day', (current_date-interval'1'day) - INTERVAL '1' month)
      and date(txn_date) >= DATE_TRUNC('month', (current_date-interval'1'day) - INTERVAL '1' month)
      --select DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' month)


      group by 1,2,3,4,5,6,7,8,9
      ) group by 1,2,3,4,5,6,7,8,9,10)





      select Flag,nsm_name,case when nsm_name = 'others' then 'others' else regional_manager_name end as regional_manager_name,BC_device_flag,city,
      state,case when nsm_name = 'others' then 'others' else region end as region,final_device_type,txn_bucket,txn_bc_percent,Total_devices,total_txn,total_bc_txn from
      (select Flag,regional_manager_name,BC_device_flag,
      city,state,region,final_device_type,txn_bucket,txn_bc_percent,Total_devices,total_txn,total_bc_txn,case when nsm_name = 'Bhavin Prajapati' then 'others' else nsm_name end as nsm_name
      from
      (
      Select * from MTD

      union all select * from LMTD))
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
    type: string
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
      nsm_name,
      regional_manager_name,
      bc_device_flag,
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
