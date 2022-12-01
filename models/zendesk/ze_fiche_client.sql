{{
  config(
    materialized = 'table',
    labels = {'type': 'zendesk', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with ze_data_client as (
     select distinct 
          link_kraken,
          user_status_ as user_status,
          case when email like '%@poiscaille%' then name_zendesk else name end as name , 
          phone_mobil,  
          user_id,
          email, 
          case when total_ca_global is null then 0 else total_ca_global end as total_ca_global,
          case when ca_global is null then 0 else ca_global end as ca_global,
          case when pan_moy is null then 0 else pan_moy end as pan_moy,
          subscription_date,
          subscription_type,
          case when subscription_status is null or subscription_status = 'Cancelled' then false else true end as subscription_status,
          last_payment,
          place_openings_day as place_openings_day_livraison,
          place_openings_day_preparation
          place_openings_schedule,
          localisation,
          place_name,
          place_address,
          place_city,
          place_codepostal,
          place_openings_day,
          place_type,
          case when nb_casiers is null then 0 else nb_casiers end as nb_casiers,
          allergies_crustaceans,
          allergies_shells,
          allergies_fishes,
          case when allergies_shells = false and allergies_crustaceans = false and allergies_fishes = false then false 
               when allergies_shells is null and allergies_crustaceans is null and allergies_fishes is null then false 
          else true end as allergie_exist,
          case when allergies_others is null then false 
               when length(allergies_others) = 0 then false 
               else true end as allergies_others,
          allergies_invalid,
          case when newsletter is null then false else true end as newsletter,
          case when refund_global_stripe is null then 0 else cast(refund_global_stripe as int64) end as refund_global_stripe,
          case when user_id_subscription is null then 'No Subscription' else user_id_subscription end as user_id_subscription,
          customer_id_stripe,
          unsubscribed_reason,
          avg_score_command_client,

          -- Date du prochain choix de casier
          format_date('%e ',next_locker_choice_date) as date_next_locker_choice_date,
          format_date('%G',next_locker_choice_date) as year_next_locker_choice_date,
          case
               when format_date('%A', next_locker_choice_date) = 'Monday' then 'Lundi'
               when format_date('%A', next_locker_choice_date) = 'Tuesday' then 'Mardi'
               when format_date('%A', next_locker_choice_date) = 'Wednesday' then 'Mecredi'
               when format_date('%A', next_locker_choice_date) = 'Thursday' then 'Jeudi'
               when format_date('%A', next_locker_choice_date) = 'Friday' then 'Vendredi'
               when format_date('%A', next_locker_choice_date) = 'Saturday' then 'Samedi'
               when format_date('%A', next_locker_choice_date) = 'Sunday' then 'Dimanche'
          end as weekday_next_locker_choice_date,

          case
               when format_date('%B', next_locker_choice_date) = 'January' then 'Janvier'
               when format_date('%B', next_locker_choice_date) = 'February' then 'Févier'
               when format_date('%B', next_locker_choice_date) = 'March' then 'Mars'
               when format_date('%B', next_locker_choice_date) = 'April' then 'Avril'
               when format_date('%B', next_locker_choice_date) = 'May' then 'Mai'
               when format_date('%B', next_locker_choice_date) = 'June' then 'Juin'
               when format_date('%B', next_locker_choice_date) = 'July' then 'Juillet'
               when format_date('%B', next_locker_choice_date) = 'August' then 'Août'
               when format_date('%B', next_locker_choice_date) = 'September' then 'Septembre'
               when format_date('%B', next_locker_choice_date) = 'October' then 'Octobre'
               when format_date('%B', next_locker_choice_date) = 'November' then 'Novembre'
               when format_date('%B', next_locker_choice_date) = 'December' then 'Décembre'
          end as month_next_locker_choice_date,
          -- format_date('%A %d %B %G',next_locker_choice_date) as next_locker_choice_date,

          -- Date de prochain préparation de casier
          format_date('%e ',next_locker_preparation_date) as date_next_locker_preparation_date,
          format_date('%G',next_locker_preparation_date) as year_next_locker_preparation_date,
          case
               when format_date('%A', next_locker_preparation_date) = 'Monday' then 'Lundi'
               when format_date('%A', next_locker_preparation_date) = 'Tuesday' then 'Mardi'
               when format_date('%A', next_locker_preparation_date) = 'Wednesday' then 'Mecredi'
               when format_date('%A', next_locker_preparation_date) = 'Thursday' then 'Jeudi'
               when format_date('%A', next_locker_preparation_date) = 'Friday' then 'Vendredi'
               when format_date('%A', next_locker_preparation_date) = 'Saturday' then 'Samedi'
               when format_date('%A', next_locker_preparation_date) = 'Sunday' then 'Dimanche'
          end as weekday_next_locker_preparation_date,

          case
               when format_date('%B', next_locker_preparation_date) = 'January' then 'Janvier'
               when format_date('%B', next_locker_preparation_date) = 'February' then 'Févier'
               when format_date('%B', next_locker_preparation_date) = 'March' then 'Mars'
               when format_date('%B', next_locker_preparation_date) = 'April' then 'Avril'
               when format_date('%B', next_locker_preparation_date) = 'May' then 'Mai'
               when format_date('%B', next_locker_preparation_date) = 'June' then 'Juin'
               when format_date('%B', next_locker_preparation_date) = 'July' then 'Juillet'
               when format_date('%B', next_locker_preparation_date) = 'August' then 'Août'
               when format_date('%B', next_locker_preparation_date) = 'September' then 'Septembre'
               when format_date('%B', next_locker_preparation_date) = 'October' then 'Octobre'
               when format_date('%B', next_locker_preparation_date) = 'November' then 'Novembre'
               when format_date('%B', next_locker_preparation_date) = 'December' then 'Décembre'
          end as month_next_locker_preparation_date,

          -- Date de prochain livraison du casier
          format_date('%e ',next_locker_delivery_date) as date_next_locker_delivery_date,
          format_date('%G',next_locker_delivery_date) as year_next_locker_delivery_date,
          case
               when format_date('%A', next_locker_delivery_date) = 'Monday' then 'Lundi'
               when format_date('%A', next_locker_delivery_date) = 'Tuesday' then 'Mardi'
               when format_date('%A', next_locker_delivery_date) = 'Wednesday' then 'Mecredi'
               when format_date('%A', next_locker_delivery_date) = 'Thursday' then 'Jeudi'
               when format_date('%A', next_locker_delivery_date) = 'Friday' then 'Vendredi'
               when format_date('%A', next_locker_delivery_date) = 'Saturday' then 'Samedi'
               when format_date('%A', next_locker_delivery_date) = 'Sunday' then 'Dimanche'
          end as weekday_next_locker_delivery_date,

          case
               when format_date('%B', next_locker_delivery_date) = 'January' then 'Janvier'
               when format_date('%B', next_locker_delivery_date) = 'February' then 'Févier'
               when format_date('%B', next_locker_delivery_date) = 'March' then 'Mars'
               when format_date('%B', next_locker_delivery_date) = 'April' then 'Avril'
               when format_date('%B', next_locker_delivery_date) = 'May' then 'Mai'
               when format_date('%B', next_locker_delivery_date) = 'June' then 'Juin'
               when format_date('%B', next_locker_delivery_date) = 'July' then 'Juillet'
               when format_date('%B', next_locker_delivery_date) = 'August' then 'Août'
               when format_date('%B', next_locker_delivery_date) = 'September' then 'Septembre'
               when format_date('%B', next_locker_delivery_date) = 'October' then 'Octobre'
               when format_date('%B', next_locker_delivery_date) = 'November' then 'Novembre'
               when format_date('%B', next_locker_delivery_date) = 'December' then 'Décembre'
          end as month_next_locker_delivery_date,

     from {{ ref('rep_clients_kpi_mongo') }} 
     --where user_status != 'lead' 
     order by user_id asc 
)


select
     * except (
          weekday_next_locker_choice_date, date_next_locker_choice_date, month_next_locker_choice_date, year_next_locker_choice_date,
          weekday_next_locker_preparation_date, date_next_locker_preparation_date, month_next_locker_preparation_date, year_next_locker_preparation_date,
          weekday_next_locker_delivery_date, date_next_locker_delivery_date, month_next_locker_delivery_date, year_next_locker_delivery_date
     ),

     concat(weekday_next_locker_choice_date,' ', date_next_locker_choice_date,' ', month_next_locker_choice_date,' ', year_next_locker_choice_date) as next_locker_choice_date,
     concat(weekday_next_locker_preparation_date,' ', date_next_locker_preparation_date,' ', month_next_locker_preparation_date,' ', year_next_locker_preparation_date) as next_locker_preparation_date,
     concat(weekday_next_locker_delivery_date,' ', date_next_locker_delivery_date,' ', month_next_locker_delivery_date,' ', year_next_locker_delivery_date) as next_locker_delivery_date
     
from ze_data_client



-- données à partir de la table sale ( start Juillet 2022)
-- CLV 