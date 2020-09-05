
# coding: utf-8

# # Objective

# This is a multinomial model that predicts the likelihood of digital subscribers to upgrade video tier on dotcom either via web or mobile.

# # Business Use Case

# This model is to be consumed by digital platform personalization team for targeting tier upgrade offers to subscribers.

# 
# # Data 
Input variables are joined using:
klondike.klondike_digital_daily + digital_modeling.omn_activity_by_visitor_dotcom + digital_modeling.omn_vw_activity_by_visitor_dotcom + digital_modeling.omn_vw_activity_by_visitor_mdotcom
tables that have both desktop and mobile dotcom. 

The target was obtained from UTC views table in NDW(NDW_EBI_UTC_VIEWS.UTC_ALL_CUSTOMER_SALES) and Rosetta(NDW_EBI_VIEWS.ADM_YYYYMM).

Months used for dev training - Sep 2019 to Dec 2019 video tier upgrades within 2 days window.

OOT1 - March 2019 
OOT2 - June 2019
# # Target Distribution

# 85.7%    - DIGITAL PREFERRED VIDEO(1)     
# 14.18%   - DIGITAL STARTER VIDEO(2)     
#  0.0008% - DIGITAL ECONOMY VIDEO(3)  {will be removed}   
# 

# # Target code sample for 1 month ( condition and where clause)
SELECT distinct utc.CUSTOMER_ACCOUNT_ID,
       utc.ACCOUNT_NUMBER,
       utc.DAY_ID,
       CAST((utc.DAY_ID (format 'YYYYMM')) AS CHAR(6)) AS YearMonth,
       VIDEO_TIER_NAME,
       PREV_VIDEO_TIER_NAME, PRODUCT_MIX
FROM NDW_EBI_UTC_VIEWS.UTC_ALL_CUSTOMER_SALES utc
  INNER JOIN (SELECT t.CUSTOMER_ACCOUNT_ID,
                     t.VIDEO_TIER_NAME,
                     t.PREV_VIDEO_TIER_NAME
              FROM (SELECT AGGR.CUSTOMER_ACCOUNT_ID,
                           CASE
                             WHEN AGGR.TIER_RANK_M0 > AGGR.TIER_RANK_M1 THEN 1
                             ELSE 0
                           END AS VIDEO_UPGRADE,
                           AGGR.VIDEO_TIER_NAME_M0 AS VIDEO_TIER_NAME,
                           AGGR.VIDEO_TIER_NAME_M1 AS PREV_VIDEO_TIER_NAME 
                    FROM (SELECT M0.CUSTOMER_ACCOUNT_ID,
                                 CASE
                                   WHEN M0.VIDEO_TIER_NAME = 'BASIC VIDEO' THEN 10
                                   WHEN M0.VIDEO_TIER_NAME = 'DIGITAL ECONOMY VIDEO' THEN 20
                                   WHEN M0.VIDEO_TIER_NAME = 'DIGITAL STARTER VIDEO' THEN 30
                                   WHEN M0.VIDEO_TIER_NAME = 'DIGITAL PREFERRED VIDEO' THEN 40
                                   ELSE NULL
                                 END AS TIER_RANK_M0,
                                 CASE
                                   WHEN M1.VIDEO_TIER_NAME = 'BASIC VIDEO' THEN 10
                                   WHEN M1.VIDEO_TIER_NAME = 'DIGITAL ECONOMY VIDEO' THEN 20
                                   WHEN M1.VIDEO_TIER_NAME = 'DIGITAL STARTER VIDEO' THEN 30
                                   WHEN M1.VIDEO_TIER_NAME = 'DIGITAL PREFERRED VIDEO' THEN 40
                                 END AS TIER_RANK_M1,
                                 M0.VIDEO_TIER_NAME AS VIDEO_TIER_NAME_M0,
                                 M1.VIDEO_TIER_NAME AS VIDEO_TIER_NAME_M1,
                                 M0.FIRST_SALES_CHANNEL_GROUP,
                                 M0.FIRST_SALES_CHANNEL,
                                 M0.VIDEO_SALES_CHANNEL_GROUP,
                                 M0.VIDEO_SALES_CHANNEL
                          FROM NDW_EBI_VIEWS.ADM_201909 M0
                            INNER JOIN NDW_EBI_VIEWS.ADM_201908 M1 
                            ON M0.CUSTOMER_ACCOUNT_ID = M1.CUSTOMER_ACCOUNT_ID
                          WHERE M0.ACCOUNT_STATUS IN ('ACT')
                          AND   M1.ACCOUNT_STATUS IN ('ACT')
                          AND   M0.RGU_VIDEO = 1
                          AND   M1.RGU_VIDEO = 1
                          AND   M1.NUMBER_OF_PRODUCTS > 0
                          AND   M0.ACCOUNT_STATUS = 'ACT'
                          AND   M0.NUMBER_OF_PRODUCTS > 0
                          AND   M0.RESIDENTIAL_IND = 1
                          AND   M0.CUSTOMER_DISCONNECT = 0
                          AND   M0.COURTESY_IND <> 1) AS AGGR) t
              WHERE t.VIDEO_UPGRADE = 1) t1 
ON utc.CUSTOMER_ACCOUNT_ID = t1.CUSTOMER_ACCOUNT_ID
WHERE (utc.DAY_ID >= '2019-09-20' AND utc.DAY_ID < '2019-10-23')
AND utc.CHANNEL LIKE '%Dot%'
AND utc.TASK_NAME LIKE 'Tier Change - Upgrade%'
AND utc.Sub_Individual_Task = 'VideoU'
# # Development dataset
create table sbalas203.dev_2019_dotcom_upto2days as 
select * from
sbalas203.dev_2019_video_upg a 
inner join digital_modeling.omn_activity_by_visitor_dotcom b 
ON a.utc_accountid=b.customer_account_id 
where datediff(cast(a.utc_day_id as date),cast(b.day_id as date))<=2
  AND datediff(cast(a.utc_day_id as date),cast(b.day_id as date))>=0 
    
set hive.support.quoted.identifiers=none;
create table sbalas203.dev_2019_dotcom_daily_upto2days as 
select * from dev_2019_dotcom_upto2days a inner join 
(select customer_account_id as customerid_renamed,
 cast(concat_ws('-', cast(lpad(year, 4, "0") as varchar(4)),cast(lpad(month, 2, "0") as varchar(2)),
                cast(lpad(`date`, 2, "0") as varchar(2))) as date) as visit_day_id_dailytable,
 `(customer_account_id)?+.+` from  klondike.klondike_digital_daily) b 
ON a.utc_accountid=b.customerid_renamed AND a.day_id=b.visit_day_id_dailytable 
where year=2019 and month>=8 and `date`>0;

create table sbalas203.dev_2019_dotcom_daily_upto2days_vws as select distinct a.utc_accountid,a.utc_account,a.utc_day_id,a.utc_yearmonth,a.utc_video_tier_name,a.prev_video_tier_name,a.total_devices,a.total_visits,a.total_pages_visited,a.customer_account_id,a.amount_paid,a.geo_city,a.geo_region,a.itg_topic,a.days_since_last_visit,a.current_mrc,a.day_id,a.visit_day_id_dailytable,a.dcom_web_num_of_visits,a.dcom_web_num_of_visits_l7d,a.dcom_web_num_of_visits_l15d,a.dcom_web_num_of_visits_l60d,a.dcom_web_num_of_visits_l90d,a.dcom_web_num_of_visits_l180d,a.dcom_web_num_of_pages,a.dcom_web_num_of_pages_l7d,a.dcom_web_num_of_pages_l15d,a.dcom_web_num_of_pages_l60d,a.dcom_web_num_of_pages_l90d,a.dcom_web_num_of_pages_l180d,a.dcom_web_timespent_seconds,a.dcom_web_timespent_seconds_l7d,a.dcom_web_timespent_seconds_l15d,a.dcom_web_timespent_seconds_l60d,a.dcom_web_timespent_seconds_l90d,a.dcom_web_timespent_seconds_l180d,a.dcom_web_num_of_purchases,a.dcom_web_num_of_purchases_7d,a.dcom_web_num_of_purchases_l15d,a.dcom_web_num_of_purchases_l60d,a.dcom_web_num_of_purchases_l90d,a.dcom_web_num_of_purchases_l180d,a.dcom_web_num_of_product_views,a.dcom_web_num_of_product_views_7d,a.dcom_web_num_of_product_views_15d,a.dcom_web_num_of_product_views_60d,a.dcom_web_num_of_product_views_90d,a.dcom_web_num_of_product_views_180d,a.dcom_web_add_to_cart_items,a.dcom_web_add_to_cart_items_7d,a.dcom_web_add_to_cart_items_15d,a.dcom_web_add_to_cart_items_60d,a.dcom_web_add_to_cart_items_90d,a.dcom_web_add_to_cart_items_180d,a.dcom_web_num_of_payments,a.dcom_web_num_of_payments_7d,a.dcom_web_num_of_payments_15d,a.dcom_web_num_of_payments_60d,a.dcom_web_num_of_payments_90d,a.dcom_web_num_of_payments_180d,a.dcom_web_move_order,a.dcom_web_move_order_l90d,a.dcom_web_move_order_l180d,a.dcom_web_move_likely,a.dcom_web_move_likely_l3d,a.dcom_web_move_likely_l7d,a.dcom_web_move_likely_l14d,a.dcom_web_move_likely_localize,a.dcom_web_move_likely_localize_l3d,a.dcom_web_move_likely_localize_l7d,a.dcom_web_move_likely_localize_l14d,a.dcom_web_move_likely_install,a.dcom_web_move_likely_install_l3d,a.dcom_web_move_likely_install_l7d,a.dcom_web_move_likely_install_l14d,a.dcom_web_move_likely_specialist,a.dcom_web_move_likely_specialist_l3d,a.dcom_web_move_likely_specialist_l7d,a.dcom_web_move_likely_specialist_l14d,a.dcom_web_move_likely_contactus,a.dcom_web_move_likely_contactus_l3d,a.dcom_web_move_likely_contactus_l7d,a.dcom_web_move_likely_contactus_l14d,a.dcom_web_move_internal_search,a.dcom_web_move_internal_search_l3d,a.dcom_web_move_internal_search_l7d,a.dcom_web_move_internal_search_l14d,a.dcom_web_buyflow_starts,a.dcom_web_buyflow_customization_step,a.dcom_web_buyflow_schedule_installation_step,a.dcom_web_buyflow_customer_info_step,a.dcom_web_buyflow_customer_order_review_step,a.dcom_web_sik_eligible,a.dcom_web_sik_option_presented,a.dcom_web_sik_selected,a.dcom_web_bill_pay,a.dcom_web_scheduled_single_pay,a.dcom_web_self_service_transaction_event,a.dcom_web_downgrade_order,a.dcom_web_call_me_back,a.dcom_web_click_to_call,a.dcom_web_products_page_views,a.dcom_web_interim_cart_add,a.dcom_web_refer_a_friend,a.dcom_web_localization,a.dcom_web_itg_starts,a.dcom_web_itg_completes,a.dcom_web_uid_creation_start,a.dcom_web_uid_username_password_creation,a.dcom_web_uid_choose_method_to_verify,a.dcom_web_uid_personal_email_verified,a.dcom_web_uid_account_lookup_complete,a.dcom_web_uid_password_recovery_complete,a.dcom_web_uid_password_reset_complete,a.dcom_web_localization_start,a.dcom_web_localization_decision,a.dcom_web_search_result_clicks,a.dcom_web_iperception_survey_launched,a.dcom_web_iperception_survey_submitted,a.dcom_web_visit_region,a.dcom_web_visit_division,a.dcom_web_dayssince_last_visit,a.dcom_mobile_num_of_visits,a.dcom_mobile_num_of_visits_l7d,a.dcom_mobile_num_of_visits_l15d,a.dcom_mobile_num_of_visits_l60d,a.dcom_mobile_num_of_visits_l90d,a.dcom_mobile_num_of_visits_l180d,a.dcom_mobile_num_of_pages,a.dcom_mobile_num_of_pages_l7d,a.dcom_mobile_num_of_pages_l15d,a.dcom_mobile_num_of_pages_l60d,a.dcom_mobile_num_of_pages_l90d,a.dcom_mobile_num_of_pages_l180d,a.dcom_mobile_timespent_seconds,a.dcom_mobile_timespent_seconds_l7d,a.dcom_mobile_timespent_seconds_l15d,a.dcom_mobile_timespent_seconds_l60d,a.dcom_mobile_timespent_seconds_l90d,a.dcom_mobile_timespent_seconds_l180d,a.dcom_mobile_num_of_purchases,a.dcom_mobile_num_of_purchases_7d,a.dcom_mobile_num_of_purchases_l15d,a.dcom_mobile_num_of_purchases_l60d,a.dcom_mobile_num_of_purchases_l90d,a.dcom_mobile_num_of_purchases_l180d,a.dcom_mobile_num_of_product_views,a.dcom_mobile_num_of_product_views_7d,a.dcom_mobile_num_of_product_views_15d,a.dcom_mobile_num_of_product_views_60d,a.dcom_mobile_num_of_product_views_90d,a.dcom_mobile_num_of_product_views_180d,a.dcom_mobile_add_to_cart_items,a.dcom_mobile_add_to_cart_items_7d,a.dcom_mobile_add_to_cart_items_15d,a.dcom_mobile_add_to_cart_items_60d,a.dcom_mobile_add_to_cart_items_90d,a.dcom_mobile_add_to_cart_items_180d,a.dcom_mobile_num_of_payments,a.dcom_mobile_num_of_payments_7d,a.dcom_mobile_num_of_payments_15d,a.dcom_mobile_num_of_payments_60d,a.dcom_mobile_num_of_payments_90d,a.dcom_mobile_num_of_payments_180d,a.dcom_mobile_move_order,a.dcom_mobile_move_order_l90d,a.dcom_mobile_move_order_l180d,a.dcom_mobile_move_likely,a.dcom_mobile_move_likely_l3d,a.dcom_mobile_move_likely_l7d,a.dcom_mobile_move_likely_l14d,a.dcom_mobile_move_likely_localize,a.dcom_mobile_move_likely_localize_l3d,a.dcom_mobile_move_likely_localize_l7d,a.dcom_mobile_move_likely_localize_l14d,a.dcom_mobile_move_likely_install,a.dcom_mobile_move_likely_install_l3d,a.dcom_mobile_move_likely_install_l7d,a.dcom_mobile_move_likely_install_l14d,a.dcom_mobile_move_likely_specialist,a.dcom_mobile_move_likely_specialist_l3d,a.dcom_mobile_move_likely_specialist_l7d,a.dcom_mobile_move_likely_specialist_l14d,a.dcom_mobile_move_likely_contactus,a.dcom_mobile_move_likely_contactus_l3d,a.dcom_mobile_move_likely_contactus_l7d,a.dcom_mobile_move_likely_contactus_l14d,a.dcom_mobile_move_internal_search,a.dcom_mobile_move_internal_search_l3d,a.dcom_mobile_move_internal_search_l7d,a.dcom_mobile_move_internal_search_l14d,a.dcom_mobile_buyflow_starts,a.dcom_mobile_buyflow_customization_step,a.dcom_mobile_buyflow_schedule_installation_step,a.dcom_mobile_buyflow_customer_info_step,a.dcom_mobile_buyflow_customer_order_review_step,a.dcom_mobile_sik_eligible,a.dcom_mobile_sik_option_presented,a.dcom_mobile_sik_selected,a.dcom_mobile_bill_pay,a.dcom_mobile_scheduled_single_pay,a.dcom_mobile_self_service_transaction_event,a.dcom_mobile_downgrade_order,a.dcom_mobile_call_me_back,a.dcom_mobile_click_to_call,a.dcom_mobile_products_page_views,a.dcom_mobile_interim_cart_add,a.dcom_mobile_refer_a_friend,a.dcom_mobile_localization,a.dcom_mobile_itg_starts,a.dcom_mobile_itg_completes,a.dcom_mobile_uid_creation_start,a.dcom_mobile_uid_username_password_creation,a.dcom_mobile_uid_choose_method_to_verify,a.dcom_mobile_uid_personal_email_verified,a.dcom_mobile_uid_account_lookup_complete,a.dcom_mobile_uid_password_recovery_complete,a.dcom_mobile_uid_password_reset_complete,a.dcom_mobile_localization_start,a.dcom_mobile_localization_decision,a.dcom_mobile_search_result_clicks,a.dcom_mobile_iperception_survey_launched,a.dcom_mobile_iperception_survey_submitted,a.dcom_mobile_visit_region,a.dcom_mobile_visit_division,a.dcom_mobile_dayssince_last_visit,a.dcom_total_num_of_visits,a.dcom_total_num_of_visits_l7d,a.dcom_total_num_of_visits_l15d,a.dcom_total_num_of_visits_l60d,a.dcom_total_num_of_visits_l90d,a.dcom_total_num_of_visits_l180d,a.dcom_total_num_of_pages,a.dcom_total_num_of_pages_l7d,a.dcom_total_num_of_pages_l15d,a.dcom_total_num_of_pages_l60d,a.dcom_total_num_of_pages_l90d,a.dcom_total_num_of_pages_l180d,a.dcom_total_timespent_seconds,a.dcom_total_timespent_seconds_l7d,a.dcom_total_timespent_seconds_l15d,a.dcom_total_timespent_seconds_l60d,a.dcom_total_timespent_seconds_l90d,a.dcom_total_timespent_seconds_l180d,a.dcom_total_num_of_purchases,a.dcom_total_num_of_purchases_l7d,a.dcom_total_num_of_purchases_l15d,a.dcom_total_num_of_purchases_l60d,a.dcom_total_num_of_purchases_l90d,a.dcom_total_num_of_purchases_l180d,a.dcom_total_num_of_product_views,a.dcom_total_num_of_product_views_l7d,a.dcom_total_num_of_product_views_l15d,a.dcom_total_num_of_product_views_l60d,a.dcom_total_num_of_product_views_l90d,a.dcom_total_num_of_product_views_l180d,a.dcom_total_add_to_cart_items,a.dcom_total_add_to_cart_items_l7d,a.dcom_total_add_to_cart_items_l15d,a.dcom_total_add_to_cart_items_l60d,a.dcom_total_add_to_cart_items_l90d,a.dcom_total_add_to_cart_items_l180d,a.dcom_total_num_of_payments,a.dcom_total_num_of_payments_l7d,a.dcom_total_num_of_payments_l15d,a.dcom_total_num_of_payments_l60d,a.dcom_total_num_of_payments_l90d,a.dcom_total_num_of_payments_l180d,a.dcom_visit_region,a.dcom_visit_divison,a.dcom_dayssince_last_visit,a.dcom_total_move_order,a.dcom_total_move_order_l90d,a.dcom_total_move_order_l180d,a.dcom_total_move_likely,a.dcom_total_move_likely_l3d,a.dcom_total_move_likely_l7d,a.dcom_total_move_likely_l14d,a.dcom_total_move_likely_localize,a.dcom_total_move_likely_localize_l3d,a.dcom_total_move_likely_localize_l7d,a.dcom_total_move_likely_localize_l14d,a.dcom_total_move_likely_install,a.dcom_total_move_likely_install_l3d,a.dcom_total_move_likely_install_l7d,a.dcom_total_move_likely_install_l14d,a.dcom_total_move_likely_specialist,a.dcom_total_move_likely_specialist_l3d,a.dcom_total_move_likely_specialist_l7d,a.dcom_total_move_likely_specialist_l14d,a.dcom_total_move_likely_contactus,a.dcom_total_move_likely_contactus_l3d,a.dcom_total_move_likely_contactus_l7d,a.dcom_total_move_likely_contactus_l14d,a.dcom_total_move_internal_search,a.dcom_total_move_internal_search_l3d,a.dcom_total_move_internal_search_l7d,a.dcom_total_move_internal_search_l14d,a.dcom_total_buyflow_starts,a.dcom_total_buyflow_customization_step,a.dcom_total_buyflow_schedule_installation_step,a.dcom_total_buyflow_customer_info_step,a.dcom_total_buyflow_customer_order_review_step,a.dcom_total_sik_eligible,a.dcom_total_sik_option_presented,a.dcom_total_sik_selected,a.dcom_total_bill_pay,a.dcom_total_scheduled_single_pay,a.dcom_total_self_service_transaction_event,a.dcom_total_downgrade_order,a.dcom_total_call_me_back,a.dcom_total_click_to_call,a.dcom_total_products_page_views,a.dcom_total_interim_cart_add,a.dcom_total_refer_a_friend,a.dcom_total_localization,a.dcom_total_itg_starts,a.dcom_total_itg_completes,a.dcom_total_uid_creation_start,a.dcom_total_uid_username_password_creation,a.dcom_total_uid_choose_method_to_verify,a.dcom_total_uid_personal_email_verified,a.dcom_total_uid_account_lookup_complete,a.dcom_total_uid_password_recovery_complete,a.dcom_total_uid_password_reset_complete,a.dcom_total_localization_start,a.dcom_total_localization_decision,a.dcom_total_search_result_clicks,a.dcom_total_iperception_survey_launched,a.dcom_total_iperception_survey_submitted,b.purchase_flag, b.product_view_flag,b.add_to_cart_flag,b.payment_flag,b.search_flag,b.campaign_flag,b.DealsoffersPage_flag,b.learnTVPage_flag,b.learnInternetPage_flag,b.learnVoicePage_flag,b.learnXfinityMobilePage_flag,b.CompareCompetitorPage_flag,b.DealsInAreaPage_flag,b.learnBundlesPage_flag,b.CompanyOverviewPage_flag,b.SpotlightAdvertiseWithUs_flag,b.CareersPage_flag,b.CustAgrmntPoliciesPage_flag,b.WatchTVOnlinePage_flag,b.CheckTVListingsPage_flag,b.LocalNewsWeatherPage_flag,b.DownloadNortonIntSecPage_flag,b.PurchaseAccessories_flag,b.HelpandSupportPage_flag,b.SubmitFeedBackPage_flag,b.ServiceCenterLocationsPage_flag,b.MoveMyServicesPage_flag,b.SelfServicePage_flag,b.ContactUsPage_flag,b.ChatonlinePage_flag,b.onDemandTVSeriesPage_flag,b.CustomerGuaranteePage_flag,b.TwitterDealsPage_flag,b.FacebookConnectWithUsPage_flag,b.TutorialsandDemosPage_flag,b.ViewPayBillPage_flag,b.checkEmailVoicemailPage_flag,b.ManageAccountPage_flag,b.GetAppsPage_flag,b.ManageParentalControlsPage_flag,b.ManageUsersPage_flag,b.ResetPasswordsPage_flag,b.BillingPage_flag,b.UpgradeServicePage_flag,b.FindUserNamePage_flag,b.AutoPayPage_flag,b.EcoBillPage_flag,b.ChannelLineUpPage_flag,b.desktop_flag,b.phone_flag,b.tablet_flag,b.chat_assistance_flag,b.hotspot_flag,c.purchase_flag as purchase_flag_m, c.product_view_flag as product_view_flag_m,c.add_to_cart_flag as add_to_cart_flag_m ,c.payment_flag as payment_flag_m ,c.search_flag as search_flag_m,c.campaign_flag as campaign_flag_m,c.DealsoffersPage_flag as DealsoffersPage_flag_m,c.learnTVPage_flag as learnTVPage_flag_m ,c.learnInternetPage_flag as learnInternetPage_flag_m ,c.learnVoicePage_flag as learnVoicePage_flag_m,c.learnXfinityMobilePage_flag as learnXfinityMobilePage_flag_m,c.CompareCompetitorPage_flag as CompareCompetitorPage_flag_m,c.DealsInAreaPage_flag as DealsInAreaPage_flag_m,c.learnBundlesPage_flag as learnBundlesPage_flag_m,c.CompanyOverviewPage_flag as CompanyOverviewPage_flag_m ,c.SpotlightAdvertiseWithUs_flag as SpotlightAdvertiseWithUs_flag_m ,c.CareersPage_flag as CareersPage_flag_m,c.CustAgrmntPoliciesPage_flag as CustAgrmntPoliciesPage_flag_m,c.WatchTVOnlinePage_flag as WatchTVOnlinePage_flag_m,c.CheckTVListingsPage_flag as CheckTVListingsPage_flag_m,c.LocalNewsWeatherPage_flag as LocalNewsWeatherPage_flag_m,c.DownloadNortonIntSecPage_flag as DownloadNortonIntSecPage_flag_m,c.PurchaseAccessories_flag as PurchaseAccessories_flag_m,c.HelpandSupportPage_flag as HelpandSupportPage_flag_m,c.SubmitFeedBackPage_flag as SubmitFeedBackPage_flag_m,c.ServiceCenterLocationsPage_flag as ServiceCenterLocationsPage_flag_m,c.MoveMyServicesPage_flag as MoveMyServicesPage_flag_m,c.SelfServicePage_flag as SelfServicePage_flag_m,c.ContactUsPage_flag as ContactUsPage_flag_m,c.ChatonlinePage_flag as ChatonlinePage_flag_m,c.onDemandTVSeriesPage_flag as onDemandTVSeriesPage_flag_m,c.CustomerGuaranteePage_flag as CustomerGuaranteePage_flag_m,c.TwitterDealsPage_flag as TwitterDealsPage_flag_m,c.FacebookConnectWithUsPage_flag as FacebookConnectWithUsPage_flag_m,c.TutorialsandDemosPage_flag as TutorialsandDemosPage_flag_m,c.ViewPayBillPage_flag as ViewPayBillPage_flag_m,c.checkEmailVoicemailPage_flag as checkEmailVoicemailPage_flag_m,c.ManageAccountPage_flag as ManageAccountPage_flag_m,c.GetAppsPage_flag as GetAppsPage_flag_m,c.ManageParentalControlsPage_flag as ManageParentalControlsPage_flag_m,c.ManageUsersPage_flag as ManageUsersPage_flag_m,c.ResetPasswordsPage_flag as ResetPasswordsPage_flag_m,c.BillingPage_flag as BillingPage_flag_m,c.UpgradeServicePage_flag as UpgradeServicePage_flag_m,c.FindUserNamePage_flag as FindUserNamePage_flag_m,c.AutoPayPage_flag as AutoPayPage_flag_m,c.EcoBillPage_flag as EcoBillPage_flag_m,c.ChannelLineUpPage_flag as ChannelLineUpPage_flag_m,c.desktop_flag as desktop_flag_m,c.phone_flag as phone_flag_m,c.tablet_flag as tablet_flag_m,c.chat_assistance_flag as chat_assistance_flag_m ,c.hotspot_flag as hotspot_flag_m,r.tenure_by_account,r.product_mix from sbalas203.dev_2019_dotcom_daily_upto2days a left join digital_modeling.omn_vw_visitor_activity_dotcom b ON a.customer_account_id=b.customer_account_id AND a.day_id=b.day_id left join digital_modeling.omn_vw_visitor_activity_mdotcom c ON a.customer_account_id=c.customer_account_id AND a.day_id=c.day_id left join base.adm_meld_201909 r ON a.customer_account_id=r.customer_account_id;
# In[1]:


warnings.filterwarnings("ignore")


# In[2]:


spark


# In[3]:


import pandas as pd
import numpy as np
from pyspark import SparkContext,HiveContext,Row,SparkConf
from pyspark.sql import *
from datetime import datetime
from numpy import *
from sklearn import preprocessing
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC
from sklearn.preprocessing import label_binarize
from sklearn.metrics import *
from sklearn import linear_model
import matplotlib.pyplot as plt
from collections import defaultdict
import warnings
from sklearn.externals import joblib
import itertools
from genetic_selection import GeneticSelectionCV
import inspect
get_ipython().run_line_magic('matplotlib', 'inline')


# In[4]:


dev_2019_dotcom_daily = spark.sql("select * from sbalas203.dev_2019_dotcom_daily_upto2days_vws")
dev_2019_dotcom_daily = dev_2019_dotcom_daily.toPandas()
dev_2019_dotcom_daily


# In[5]:


list(dev_2019_dotcom_daily.columns)


# In[6]:


#dev_2019_dotcom_daily["day_id"].value_counts()
nulls = dev_2019_dotcom_daily.isnull().sum()
dev_2019_dotcom_daily.columns[dev_2019_dotcom_daily.isnull().any()]


# In[7]:


dev_2019_dotcom_daily['prev_video_tier_name'].value_counts()


# In[8]:


dev_2019_dotcom_daily.drop(dev_2019_dotcom_daily[dev_2019_dotcom_daily['utc_video_tier_name'] == 'DIGITAL ECONOMY VIDEO'].index,inplace = True) 


# In[9]:


data = dev_2019_dotcom_daily
data['utc_video_tier_name'].value_counts()


# In[10]:


data['target'] = data.utc_video_tier_name.apply(lambda x: 1 if x== 'DIGITAL PREFERRED VIDEO' 
                                                else 0)


# In[11]:


data['prev_video_tier_name_enc'] = data.prev_video_tier_name.apply(lambda x: 1 if x== 'DIGITAL STARTER VIDEO' 
                                                else (2 if x=='DIGITAL ECONOMY VIDEO' else 3))


# In[12]:


data['prev_video_tier_name_enc'].value_counts()


# In[13]:


data['productmix'] = data.product_mix.apply(lambda x: 1 if x== 'VIDEO ONLY' 
                                                else 2 if x=='VIDEO/HSD'
                                                else 3 if x=='VIDEO/HSD/CDV'
                                                else 4 if x=='VIDEO/HSD/XH'
                                                else 5 if x=='VIDEO/HSD/CDV/XH' else 0)


# In[14]:


data['productmix'].value_counts()


# In[15]:


data['target'].value_counts()


# In[16]:


data = data.replace("None", 0)
data = data.replace("Y",1)
data = data.replace("N",0)
data = data.replace(" ",0)
data = data.replace(np.nan,0)
data = data.fillna('0')


# In[17]:


data['tenure_by_account'] = data.tenure_by_account.apply(lambda x: 75 if x>74 
                                                else x)


# In[18]:


target = 'target'
data[target]
data[target] = data[target].astype('int')


# In[19]:


num = data.select_dtypes(include=['float64','int64']).columns
cat = data.select_dtypes(include=['O']).columns


# In[20]:


num, cat


# In[21]:


# works for numerical columns only
s_num = pd.DataFrame(data[num].describe())
s_num.ix['max_min'] = s_num.ix['max'] - s_num.ix['min']
s_num.ix['missing'] = ((len(data[num]) - s_num.ix['count'])/len(data[num]))*100
st = s_num.transpose()
st[st['missing']>50]


# In[22]:


var_drop_list = ['utc_accountid','utc_account','utc_day_id','utc_yearmonth','days_since_last_visit',
 'utc_video_tier_name','customer_account_id','geo_city','geo_region','day_id','current_mrc',
'visit_day_id_dailytable','dcom_web_visit_division','dcom_web_visit_region','dcom_mobile_visit_region',
        'dcom_mobile_visit_division', 'dcom_visit_region','itg_topic',
        'dcom_visit_divison','amount_paid','prev_video_tier_name','product_mix','dcom_web_num_of_visits_l15d',
 'dcom_web_num_of_visits_l60d','dcom_web_num_of_visits_l90d','dcom_web_num_of_visits_l180d','dcom_web_num_of_pages_l15d',
 'dcom_web_num_of_pages_l60d','dcom_web_num_of_pages_l90d','dcom_web_num_of_pages_l180d','dcom_web_timespent_seconds_l15d',
 'dcom_web_timespent_seconds_l60d','dcom_web_timespent_seconds_l90d','dcom_web_timespent_seconds_l180d','dcom_web_num_of_purchases_l15d',
 'dcom_web_num_of_purchases_l60d','dcom_web_num_of_purchases_l90d','dcom_web_num_of_purchases_l180d',
 'dcom_web_num_of_product_views_60d','dcom_web_num_of_product_views_90d','dcom_web_num_of_product_views_180d',
 'dcom_web_add_to_cart_items_60d','dcom_web_add_to_cart_items_90d','dcom_web_add_to_cart_items_180d','dcom_web_num_of_payments_60d','dcom_web_num_of_payments_90d','dcom_web_num_of_payments_180d',
 'dcom_web_move_order_l90d','dcom_web_move_order_l180d','dcom_web_move_likely_l14d',
                 'dcom_web_move_likely_localize_l14d','dcom_web_move_likely_install_l14d',
                 'dcom_web_move_likely_specialist','dcom_web_move_likely_specialist_l14d','dcom_web_move_likely_contactus_l14d','dcom_web_move_internal_search_l14d',
                'dcom_mobile_num_of_visits',
 'dcom_mobile_num_of_visits_l15d',
 'dcom_mobile_num_of_visits_l60d',
 'dcom_mobile_num_of_visits_l90d',
 'dcom_mobile_num_of_visits_l180d',
 'dcom_mobile_num_of_pages',
 'dcom_mobile_num_of_pages_l15d',
 'dcom_mobile_num_of_pages_l60d',
 'dcom_mobile_num_of_pages_l90d',
 'dcom_mobile_num_of_pages_l180d',
 'dcom_mobile_timespent_seconds',
 'dcom_mobile_timespent_seconds_l15d',
 'dcom_mobile_timespent_seconds_l60d',
 'dcom_mobile_timespent_seconds_l90d',
 'dcom_mobile_timespent_seconds_l180d',
 'dcom_mobile_num_of_purchases',
 'dcom_mobile_num_of_purchases_l15d',
 'dcom_mobile_num_of_purchases_l60d',
 'dcom_mobile_num_of_purchases_l90d',
 'dcom_mobile_num_of_purchases_l180d',
 'dcom_mobile_num_of_product_views',
 'dcom_mobile_num_of_product_views_15d',
 'dcom_mobile_num_of_product_views_60d',
 'dcom_mobile_num_of_product_views_90d',
 'dcom_mobile_num_of_product_views_180d',
 'dcom_mobile_add_to_cart_items',
 'dcom_mobile_add_to_cart_items_15d',
 'dcom_mobile_add_to_cart_items_60d',
 'dcom_mobile_add_to_cart_items_90d',
 'dcom_mobile_add_to_cart_items_180d',
 'dcom_mobile_num_of_payments',
 'dcom_mobile_num_of_payments_60d',
 'dcom_mobile_num_of_payments_90d',
 'dcom_mobile_num_of_payments_180d',
 'dcom_mobile_move_order',
 'dcom_mobile_move_order_l90d',
 'dcom_mobile_move_order_l180d',
 'dcom_mobile_move_likely',
 'dcom_mobile_move_likely_l14d',
 'dcom_mobile_move_likely_localize',
 'dcom_mobile_move_likely_localize_l14d',
 'dcom_mobile_move_likely_install',
 'dcom_mobile_move_likely_install_l14d',
 'dcom_mobile_move_likely_specialist',
 'dcom_mobile_move_likely_specialist_l14d',
 'dcom_mobile_move_likely_contactus',
 'dcom_mobile_move_likely_contactus_l14d',
 'dcom_mobile_move_internal_search',
 'dcom_mobile_move_internal_search_l14d',
 'dcom_total_num_of_visits',
 'dcom_total_num_of_visits_l15d',
 'dcom_total_num_of_visits_l60d',
 'dcom_total_num_of_visits_l90d',
 'dcom_total_num_of_visits_l180d',
 'dcom_total_num_of_pages',
 'dcom_total_num_of_pages_l15d',
 'dcom_total_num_of_pages_l60d',
 'dcom_total_num_of_pages_l90d',
 'dcom_total_num_of_pages_l180d',
 'dcom_total_timespent_seconds',
 'dcom_total_timespent_seconds_l15d',
 'dcom_total_timespent_seconds_l60d',
 'dcom_total_timespent_seconds_l90d',
 'dcom_total_timespent_seconds_l180d',
 'dcom_total_num_of_purchases',
 'dcom_total_num_of_purchases_l15d',
 'dcom_total_num_of_purchases_l60d',
 'dcom_total_num_of_purchases_l90d',
 'dcom_total_num_of_purchases_l180d',
 'dcom_total_num_of_product_views',
 'dcom_total_num_of_product_views_l15d',
 'dcom_total_num_of_product_views_l60d',
 'dcom_total_num_of_product_views_l90d',
 'dcom_total_num_of_product_views_l180d',
 'dcom_total_add_to_cart_items',
 'dcom_total_add_to_cart_items_l15d',
 'dcom_total_add_to_cart_items_l60d',
 'dcom_total_add_to_cart_items_l90d',
 'dcom_total_add_to_cart_items_l180d',
 'dcom_total_num_of_payments',
 'dcom_total_num_of_payments_l15d',
 'dcom_total_num_of_payments_l60d',
 'dcom_total_num_of_payments_l90d',
 'dcom_total_num_of_payments_l180d',
 'dcom_dayssince_last_visit',
 'dcom_total_move_order',
 'dcom_total_move_order_l90d',
 'dcom_total_move_order_l180d',
 'dcom_total_move_likely',
 'dcom_total_move_likely_l14d',
 'dcom_total_move_likely_localize',
 'dcom_total_move_likely_localize_l14d',
 'dcom_total_move_likely_install',
 'dcom_total_move_likely_install_l14d',
 'dcom_total_move_likely_specialist',
 'dcom_total_move_likely_specialist_l14d',
 'dcom_total_move_likely_contactus',
 'dcom_total_move_likely_contactus_l14d',
 'dcom_total_move_internal_search',
 'dcom_total_move_internal_search_l14d',]
print ("Dropped variables :", len(var_drop_list))


# In[23]:


#correlation = data[var_names].corr(method='pearson')
#correlation[correlation['target']>0.015]


# In[24]:


data = data.copy()
for x in data.columns:
    try:
        data[x] = data[x].astype(float)
    except:
        continue


# # OOT Dataset

# In[25]:


oot_201906_dotcom_daily = spark.sql("select * from sbalas203.oot_201906_dotcom_daily_upto2days_vws")
oot_201906_dotcom_daily = oot_201906_dotcom_daily.toPandas()
oot_201906_dotcom_daily


# In[26]:


oot_201906_dotcom_daily.drop(oot_201906_dotcom_daily[oot_201906_dotcom_daily['utc_video_tier_name'] == 'DIGITAL ECONOMY VIDEO'].index,inplace = True) 


# In[27]:


oot201906 = oot_201906_dotcom_daily
oot201906['utc_video_tier_name'].value_counts()


# In[28]:


oot201906['target'] = oot201906.utc_video_tier_name.apply(lambda x: 1 if x== 'DIGITAL PREFERRED VIDEO' 
                                                else 0)


# In[29]:


oot201906['prev_video_tier_name_enc'] = oot201906.prev_video_tier_name.apply(lambda x: 1 if x== 'DIGITAL STARTER VIDEO' 
                                                else (2 if x=='DIGITAL ECONOMY VIDEO' else 3))


# In[30]:


oot201906['productmix'] = oot201906.product_mix.apply(lambda x: 1 if x== 'VIDEO ONLY' 
                                                else 2 if x=='VIDEO/HSD'
                                                else 3 if x=='VIDEO/HSD/CDV'
                                                else 4 if x=='VIDEO/HSD/XH'
                                                else 5 if x=='VIDEO/HSD/CDV/XH' else 0)


# In[31]:


oot201906 = oot201906.replace("None", 0)
oot201906 = oot201906.replace("Y",1)
oot201906 = oot201906.replace("N",0)
oot201906 = oot201906.replace(" ",0)
oot201906 = oot201906.replace(np.nan,0)
oot201906 = oot201906.fillna('0')


# In[32]:


target = 'target'
oot201906[target]
oot201906[target] = oot201906[target].astype('int')


# In[33]:


oot201906['tenure_by_account'] = oot201906.tenure_by_account.apply(lambda x: 75 if x>74 
                                                else x)


# In[34]:


# works for numerical columns only
s_num = pd.DataFrame(oot201906[num].describe())
s_num.ix['max_min'] = s_num.ix['max'] - s_num.ix['min']
s_num.ix['missing'] = ((len(oot201906[num]) - s_num.ix['count'])/len(oot201906[num]))*100
st = s_num.transpose()
st[st['missing']>50]


# In[35]:


oot201906 = oot201906.copy()
for x in oot201906.columns:
    try:
        oot201906[x] = oot201906[x].astype(float)
    except:
        continue


# In[36]:


#OOT 201903 
oot_201903_dotcom_daily = spark.sql("select * from sbalas203.oot_201903_dotcom_daily_upto2days_vws")
oot_201903_dotcom_daily = oot_201903_dotcom_daily.toPandas()
oot_201903_dotcom_daily


# In[37]:


oot_201903_dotcom_daily.drop(oot_201903_dotcom_daily[oot_201903_dotcom_daily['utc_video_tier_name'] 
                                                     == 'DIGITAL ECONOMY VIDEO'].index,inplace = True) 


# In[38]:


oot201903 = oot_201903_dotcom_daily
oot201903['utc_video_tier_name'].value_counts()


# In[39]:


oot201903['target'] = oot201903.utc_video_tier_name.apply(lambda x: 1 if x== 'DIGITAL PREFERRED VIDEO' else 0)
oot201903['prev_video_tier_name_enc'] = oot201903.prev_video_tier_name.apply(lambda x: 1 if x== 'DIGITAL STARTER VIDEO' 
                                                else (2 if x=='DIGITAL ECONOMY VIDEO' else 3))
oot201903['productmix'] = oot201903.product_mix.apply(lambda x: 1 if x== 'VIDEO ONLY' 
                                                else 2 if x=='VIDEO/HSD'
                                                else 3 if x=='VIDEO/HSD/CDV'
                                                else 4 if x=='VIDEO/HSD/XH'
                                                else 5 if x=='VIDEO/HSD/CDV/XH' else 0)


# In[40]:


oot201903 = oot201903.replace("None", 0)
oot201903 = oot201903.replace("Y",1)
oot201903 = oot201903.replace("N",0)
oot201903 = oot201903.replace(" ",0)
oot201903 = oot201903.replace(np.nan,0)
oot201903 = oot201903.fillna('0')


# In[41]:


oot201903['tenure_by_account'] = oot201903.tenure_by_account.apply(lambda x: 75 if x>74 
                                                else x)


# In[42]:


target = 'target'
oot201903[target]
oot201903[target] = oot201903[target].astype('int')


# In[43]:


# works for numerical columns only
s_num = pd.DataFrame(oot201903[num].describe())
s_num.ix['max_min'] = s_num.ix['max'] - s_num.ix['min']
s_num.ix['missing'] = ((len(oot201903[num]) - s_num.ix['count'])/len(oot201903[num]))*100
st = s_num.transpose()
st[st['missing']>50]


# In[44]:


oot201903 = oot201903.copy()
for x in oot201903.columns:
    try:
        oot201903[x] = oot201903[x].astype(float)
    except:
        continue


# In[45]:


def encoder(df, var_list):
    
    m = defaultdict(preprocessing.LabelEncoder)
    enc_res = df[var_list].fillna('NA').replace(' ','NA').apply(lambda x: x.astype(str).str.upper()).apply(lambda x: m[x.name].fit_transform(x))
    enc_res.columns = [str(x) +'_LE' for x in enc_res.columns]
    
    return enc_res, m


# In[46]:


def woe_iv(df,var_list,target):
    labels = ['1', '2', '3']
    yl = label_binarize(array(df[target]), classes=[1,2,3])
    yl = pd.DataFrame(yl, columns = labels)
    df = df.drop(target, axis =1)
    df = pd.concat([df, yl], axis = 1)
    r = []
    iv = []
    m = []
    for i in range(len(var_list)):
        iv.append([])
        iv[i].append(var_list[i])
        r0=[]
        r1=[]
        
        for j in range(len(labels)):
            v = df.groupby([var_list[i],labels[j]]).size().unstack()
            g = v.apply(lambda x: x/float(x.sum())).fillna(0)
            r0.append(g[0])
            r1.append(g[1])
        ne = pd.DataFrame(r0)
        e = pd.DataFrame(r1)
        
        v['Non event_pct'] = ne.mean()
        v['Event_pct'] = e.mean()
        v['WOE'] = (log((e.mean()+0.001)/(ne.mean()+0.001)))
        v['IV'] = (e.mean()-ne.mean())*v['WOE']
        m.append(pd.DataFrame(v['WOE']).reset_index())
        r.append(v)
        iv[i].append(v['IV'].sum())
    iv=pd.DataFrame(iv,columns = ['Variable_name','IV']).sort_values('IV', ascending = 0)
    return (r, iv, m) 


# In[47]:


def cor_cat_var(df, woe_df, var_list):
    res = pd.DataFrame()
    fin_cat = pd.DataFrame()
    
    for i in range(len(var_list)):
        woe = pd.DataFrame({var_list[i]+'_WOE' : woe_df[i]['WOE'].values,var_list[i]: woe_df[i]['WOE'].index})
        var = pd.DataFrame(df.iloc[:,i]).reset_index().merge(woe).set_index('index')
        res = pd.concat([res,var],axis=1).reindex(var.index)
    
    for j in range(1,len(res.columns),2):
        fin_cat = pd.concat([fin_cat, pd.DataFrame(res.iloc[:,j])],axis = 1)
    
    return res, fin_cat


# In[48]:


def data_prep(df, var_drop, target):
    
    num = df.select_dtypes(include=['float64','int64']).columns
    cat = df.select_dtypes(include=['O']).columns
    
    cat_var = [x for x in list(cat) if x in var_drop]
    num_var = [x for x in list(num) if x in var_drop]
    
    num_var = num.drop(num_var)
    cat_var = cat.drop(cat_var)
    
    num_df = pd.concat([df[num_var].fillna(0), pd.DataFrame(df[target])], axis=1)
    cat_df = pd.concat([df[cat_var].fillna('NA').replace(' ','NA').apply(lambda x: x.astype(str).str.upper()),pd.DataFrame(df[target])], axis=1)

    return num_df, cat_df, num_var, cat_var


# In[49]:


def metrics(y_true, y_pred, classes, avg):
    
    auc = accuracy_score(y_true, y_pred)
    p, r, f, _ = precision_recall_fscore_support(y_true, y_pred, labels=classes, average = avg)
    
    cm = confusion_matrix(y_true, y_pred)
    set_printoptions(precision=2)
     
    return auc, p, r, f, cm


# In[50]:


def plot_confusion_matrix(cm, classes,
                              normalize=True,
                              title='Confusion matrix',
                              cmap=plt.cm.Blues):
        """
        This function prints and plots the confusion matrix.
        Normalization can be applied by setting `normalize=True`.
        """
        plt.imshow(cm, interpolation='nearest', cmap=cmap)
        plt.title(title)
        plt.colorbar()
        tick_marks = arange(len(classes))
        plt.xticks(tick_marks, classes)#, rotation=45)
        plt.yticks(tick_marks, classes)
        
        thresh = cm.max() / 2.
        for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
            plt.text(j, i, cm[i, j],
                     horizontalalignment="center",
                     color="white" if cm[i, j] > thresh else "black")

        plt.tight_layout()
        plt.ylabel('True label')
        plt.xlabel('Predicted label')
        
        set_printoptions(suppress=True)

        #if normalize:
        cm = cm.astype('float') / cm.sum(axis=1)[:, newaxis]
        print("\n" + "Normalized confusion matrix")
        #else:print "\n" + ('Confusion matrix, without normalization')

        print (cm)


# In[51]:


def out_ks_c(X, Y, obj):
    
    yl = label_binarize(array(Y), classes=[1, 2, 3])
    obj = obj.fit(X, yl)
    y_pred = obj.predict(X)
    y_proba = obj.predict_proba(X)

    n_classes = yl.shape[1]

    for j in range(n_classes):
        outp = pd.DataFrame(yl[:,j], columns = ["Target"])
        outp['Proba'] = y_proba[j][:,1]
        outp['Pred'] = y_pred[:,j]
        outp['Decile'] = pd.qcut((outp['Proba'].rank(method='first')), 10, labels = arange(10,0,-1))

        #fin_res = pd.DataFrame(arange(10,0,-1),columns = ['Decile'])
        fin_res = pd.DataFrame(arange(1,11),columns = ['Decile'])
        fin_res['Count']=(outp.groupby(['Decile'],as_index=False).count())['Pred']
        fin_res['Target']=(outp.loc[outp['Target'] == 1].groupby(['Decile'],as_index=False).count())['Pred']
        fin_res['Non_Target']=(outp.loc[outp['Target'] == 0].groupby(['Decile'],as_index=False).count())['Pred']
        fin_res['Prob_Target']=(outp.groupby(['Decile'],as_index=False).sum())['Proba']
        fin_res['1_pct'] = (fin_res['Target']/sum(fin_res['Target']))
        fin_res['0_pct'] = (fin_res['Non_Target']/sum(fin_res['Non_Target']))
        #fin_res = fin_res.iloc[::-1]
        fin_res['1_cum'], fin_res['0_cum'], fin_res['Lift'] = " ", " ", " "
        r1 = 0
        r0 = 0
        #for i in range(9,-1,-1):
        for i in range(10):
            r1 = r1 + fin_res['1_pct'][i]
            r0 = r0 + fin_res['0_pct'][i]
            fin_res['1_cum'][i] = r1
            fin_res['0_cum'][i] = r0
            #fin_res['Lift'][i] = r1 / (10-i)*10
            fin_res['Lift'][i] = r1 / (i+1)*10
        fin_res['KS'] = abs(fin_res['1_cum'] - fin_res['0_cum'])*100
        ks = max(fin_res['KS'])
        print ("KS of class " + str(j+1) + " is : ", ks)
        yield fin_res
        
def out_ks(df, Y, obj):
    
    outp = pd.DataFrame(Y)
    outp['Pred'] = obj.predict(df)
    outp['Proba'] = obj.predict_proba(df)[:,1]
           
    return (outp['Pred'], outp['Proba'])


# In[52]:


cat_var = [x for x in list(cat) if x in var_drop_list]
num_var = [x for x in list(num) if x in var_drop_list]

num_var = num.drop(num_var)
cat_var = cat.drop(cat_var)

#df[target] = df[target].astype(int)
num_dev = pd.concat([data[num_var], pd.DataFrame(data[target])], axis=1)
cat_dev = pd.concat([data[cat_var], pd.DataFrame(data[target])], axis=1)

print ("Numerical dataframe :", num_dev.shape)
print ("Categorical dataframe :",cat_dev.shape)


# In[53]:


############## step data preparation ##############
# weight of evidence
#woe_data, iv, woe_map = woe_iv(cat_df, cat_var, target)
#result, final_catg = cor_cat_var(cat_df, woe_data, cat_var)
# label encoding
#enc_res, enc_map = encoder(cat_df, cat_var)
# concatenating numerical data, woe data, encoded data to form final dataset
#final_data = pd.concat([num_df, final_catg, enc_res], axis = 1).reindex(final_catg.index)
#var_names = final_data.columns.drop(target)


# In[54]:


var_names = data.columns.drop(var_drop_list)
list(var_names)


# In[55]:


#numeric imputation
num_df = num_dev.fillna(99999)


# In[56]:


#categorical imputation
cat_df = cat_dev.fillna('NA')
s_catt = pd.DataFrame(cat_df.isnull().sum()/len(cat_df)*100, columns = ['missing'])
s_catt[s_catt['missing']>0]


# In[57]:


data = data[['prev_video_tier_name_enc',  
                            'productmix',   
                     'tenure_by_account' ,   
           'dcom_total_num_of_pages_l7d' , 
      'dcom_total_timespent_seconds_l7d' ,   
          'dcom_total_num_of_visits_l7d' ,   
   'dcom_total_num_of_product_views_l7d' ,  
                   'product_view_flag' ,   
                  'dealsofferspage_flag' ,   
                    'add_to_cart_flag' ,  
     'dcom_total_add_to_cart_items_l7d' ,   
        'dcom_web_dayssince_last_visit','target']]
data = data.drop_duplicates()


# In[92]:


data = data[['dcom_web_num_of_visits_l7d', 'dcom_web_num_of_product_views_7d',
       'dcom_web_num_of_payments_15d', 'dcom_web_move_likely_specialist_l3d',
       'dcom_web_bill_pay', 'dcom_web_downgrade_order',
       'dcom_web_call_me_back', 'dcom_web_localization',
       'dcom_web_uid_personal_email_verified',
       'dcom_web_uid_account_lookup_complete',
       'dcom_mobile_timespent_seconds_l7d',
       'dcom_mobile_num_of_product_views_7d',
       'dcom_mobile_num_of_payments_15d', 'dcom_mobile_move_likely_l7d',
       'dcom_mobile_move_likely_specialist_l3d', 'dcom_mobile_buyflow_starts',
       'dcom_mobile_sik_eligible', 'dcom_mobile_bill_pay',
       'dcom_mobile_itg_completes', 'dcom_mobile_uid_password_reset_complete',
       'dcom_mobile_localization_decision', 'dcom_total_num_of_payments_l7d',
       'dcom_total_move_likely_specialist_l3d',
       'dcom_total_move_likely_specialist_l7d',
       'dcom_total_move_internal_search_l3d',
       'dcom_total_buyflow_customer_order_review_step',
       'dcom_total_scheduled_single_pay', 'dcom_total_itg_completes',
       'dcom_total_uid_username_password_creation', 'learntvpage_flag',
       'careerspage_flag', 'watchtvonlinepage_flag',
       'downloadnortonintsecpage_flag', 'manageaccountpage_flag',
       'upgradeservicepage_flag', 'search_flag_m', 'campaign_flag_m',
       'comparecompetitorpage_flag_m', 'careerspage_flag_m',
       'servicecenterlocationspage_flag_m', 'contactuspage_flag_m',
       'prev_video_tier_name_enc','target']]
data = data.drop_duplicates()


# In[101]:


#from genetic algorithm
var_names = ['dcom_web_num_of_visits_l7d', 'dcom_web_num_of_product_views_7d',
       'dcom_web_num_of_payments_15d', 'dcom_web_move_likely_specialist_l3d',
       'dcom_web_bill_pay', 'dcom_web_downgrade_order',
       'dcom_web_call_me_back', 'dcom_web_localization',
       'dcom_web_uid_personal_email_verified',
       'dcom_web_uid_account_lookup_complete',
       'dcom_mobile_timespent_seconds_l7d',
       'dcom_mobile_num_of_product_views_7d',
       'dcom_mobile_num_of_payments_15d', 'dcom_mobile_move_likely_l7d',
       'dcom_mobile_move_likely_specialist_l3d', 'dcom_mobile_buyflow_starts',
       'dcom_mobile_sik_eligible', 'dcom_mobile_bill_pay',
       'dcom_mobile_itg_completes', 'dcom_mobile_uid_password_reset_complete',
       'dcom_mobile_localization_decision', 'dcom_total_num_of_payments_l7d',
       'dcom_total_move_likely_specialist_l3d',
       'dcom_total_move_likely_specialist_l7d',
       'dcom_total_move_internal_search_l3d',
       'dcom_total_buyflow_customer_order_review_step',
       'dcom_total_scheduled_single_pay', 'dcom_total_itg_completes',
       'dcom_total_uid_username_password_creation', 'learntvpage_flag',
       'careerspage_flag', 'watchtvonlinepage_flag',
       'downloadnortonintsecpage_flag', 'manageaccountpage_flag',
       'upgradeservicepage_flag',
       'prev_video_tier_name_enc']


# In[58]:


var_names = ['prev_video_tier_name_enc','productmix','dcom_total_num_of_pages_l7d',
 'dcom_total_num_of_visits_l7d','dcom_total_add_to_cart_items_l7d'
,'tenure_by_account','dcom_total_num_of_purchases_l7d'
,'dcom_total_timespent_seconds_l7d'
,'dcom_total_num_of_product_views_l7d'   
,'dcom_web_dayssince_last_visit','purchase_flag',
 'product_view_flag',
 'add_to_cart_flag',
 'payment_flag',
 'search_flag',
 'campaign_flag',
 'dealsofferspage_flag',
 'learntvpage_flag',
 'learninternetpage_flag',
 'learnvoicepage_flag',
 'learnxfinitymobilepage_flag',
 'comparecompetitorpage_flag',
 'dealsinareapage_flag',
 'learnbundlespage_flag']


# In[60]:


# split the development file into training, test datasets in the ratio 80:20 resp.
X_train, X_test, Y_train, Y_test = train_test_split(data[var_names], data[target], test_size=0.20,random_state=3)
print (X_train.shape)
print (X_test.shape)

train = pd.concat([X_train, Y_train], axis = 1)
test = pd.concat([X_test, Y_test], axis = 1)
train.shape, test.shape


# In[65]:


#final_data.write.saveAsTable('sbalas203.final_data')


# In[66]:


#dev_2019["accountid"] = dev_2019.astype({'accountid': 'object'})
#dev_2019.dtypes


# In[61]:


X_train.columns


# In[62]:


def model_fit(X, Y, obj, size):
    i = 0
    var_list = X.columns
    while len(var_list) > size:
        obj.fit(X[var_list],Y)
        var_imp = pd.DataFrame(list(zip(X[var_list], obj.feature_importances_)), columns = ['Var_name', 'Importance'])            .sort_values(['Importance'], ascending = [0])    
        redu_feat = var_imp[var_imp['Importance']>min(var_imp['Importance'])]
        i = i+1
       # print ("Iteration "+ str(i) + " : No. of original features are: ", len(X[var_list].columns))
       # print ("Iteration "+ str(i) + " : No. of reduced features are: ", len(redu_feat))
        var_list = redu_feat['Var_name']
    print ("Iteration "+ str(i) + " : No. of reduced features are: ", len(redu_feat))
    return redu_feat


# In[242]:


rnf = RandomForestClassifier(n_estimators=200, max_features='sqrt', n_jobs=-1, max_depth=4)
# Variable selection based on random forest's variable importance
start_time = datetime.now()
redu = model_fit(X_train, Y_train, rnf, size=12)
print (datetime.now() - start_time)
print ("Final variables of the model are: " + "\n", redu.reset_index(drop=True))


# In[69]:


oot201903['tenure_by_account'].hist()


# In[107]:


x_f = data[redu['Var_name']]
vif_cat_f = pd.DataFrame()
len(x_f.columns)
#x_f.columns


# In[68]:


from statsmodels.stats.outliers_influence import variance_inflation_factor
vif_drop_var_f = []
vif_drop_val_f = []

startTime = datetime.now()
vif_f = [variance_inflation_factor(x_f.values, ix) for ix in range(x_f.shape[1])]
vif_cat_f = pd.concat([pd.DataFrame(x_f.columns, columns = ['Var_name']), pd.DataFrame(vif_f, columns = ['VIF'])],axis=1)
vif_cat_f = vif_cat_f.sort_values('VIF', ascending=0).reset_index(drop=True)
print (datetime.now() - startTime)
print ("Final no. of categorical variables after VIF based reduction : ", len(vif_cat_f))


while (vif_cat_f['VIF'][0] >=10):
    x_f = data[redu['Var_name']]
    vif_drop_var_f.append(vif_cat_f['Var_name'][0])
    vif_drop_val_f.append(vif_cat_f['VIF'][0])
    x_f = x_f.drop(vif_drop_var_f, axis = 1)

    startTime = datetime.now()
    vif_f = [variance_inflation_factor(x_f.values, ix) for ix in range(x_f.shape[1])]
    vif_cat_f = pd.concat([pd.DataFrame(x_f.columns, columns = ['Var_name']), pd.DataFrame(vif_f, columns = ['VIF'])],axis=1)
    vif_cat_f = vif_cat_f.sort_values('VIF', ascending=0).reset_index(drop=True)
    print (datetime.now() - startTime)
    print ("Final no. of categorical variables after VIF based reduction : ", len(vif_cat_f))
    
    vif_drop_f = pd.DataFrame({'VIF':vif_drop_val_f,'Var_name':vif_drop_var_f})
    

print(vif_drop_f)
print(vif_cat_f)


# In[76]:


vif_cat_f['Var_name']


# In[59]:


# after variable reduction
var_names = ['prev_video_tier_name_enc',  
                            'productmix',   
                     'tenure_by_account' ,   
           'dcom_total_num_of_pages_l7d' , 
      'dcom_total_timespent_seconds_l7d' ,   
          'dcom_total_num_of_visits_l7d' ,   
   'dcom_total_num_of_product_views_l7d' ,  
                   'product_view_flag' ,   
                  'dealsofferspage_flag' ,   
                    'add_to_cart_flag' ,  
     'dcom_total_add_to_cart_items_l7d' ,   
        'dcom_web_dayssince_last_visit']


# In[58]:


#OOT remove duplicates
oot201906_original = oot201906
oot201903_original = oot201903
oot201906 = oot201906[['prev_video_tier_name_enc',  
                            'productmix',   
                     'tenure_by_account' ,   
           'dcom_total_num_of_pages_l7d' , 
      'dcom_total_timespent_seconds_l7d' ,   
          'dcom_total_num_of_visits_l7d' ,   
   'dcom_total_num_of_product_views_l7d' ,  
                   'product_view_flag' ,   
                  'dealsofferspage_flag' ,   
                    'add_to_cart_flag' ,  
     'dcom_total_add_to_cart_items_l7d' ,   
        'dcom_web_dayssince_last_visit','target']]
oot201906 = oot201906.drop_duplicates()

oot201903 = oot201903[['prev_video_tier_name_enc',  
                            'productmix',   
                     'tenure_by_account' ,   
           'dcom_total_num_of_pages_l7d' , 
      'dcom_total_timespent_seconds_l7d' ,   
          'dcom_total_num_of_visits_l7d' ,   
   'dcom_total_num_of_product_views_l7d' ,  
                   'product_view_flag' ,   
                  'dealsofferspage_flag' ,   
                    'add_to_cart_flag' ,  
     'dcom_total_add_to_cart_items_l7d' ,   
        'dcom_web_dayssince_last_visit','target']]
oot201903 = oot201903.drop_duplicates()


# In[102]:


#OOT remove duplicates for genetic algo purpose
oot201906_original = oot201906
oot201903_original = oot201903
oot201906 = oot201906[['dcom_web_num_of_visits_l7d', 'dcom_web_num_of_product_views_7d',
       'dcom_web_num_of_payments_15d', 'dcom_web_move_likely_specialist_l3d',
       'dcom_web_bill_pay', 'dcom_web_downgrade_order',
       'dcom_web_call_me_back', 'dcom_web_localization',
       'dcom_web_uid_personal_email_verified',
       'dcom_web_uid_account_lookup_complete',
       'dcom_mobile_timespent_seconds_l7d',
       'dcom_mobile_num_of_product_views_7d',
       'dcom_mobile_num_of_payments_15d', 'dcom_mobile_move_likely_l7d',
       'dcom_mobile_move_likely_specialist_l3d', 'dcom_mobile_buyflow_starts',
       'dcom_mobile_sik_eligible', 'dcom_mobile_bill_pay',
       'dcom_mobile_itg_completes', 'dcom_mobile_uid_password_reset_complete',
       'dcom_mobile_localization_decision', 'dcom_total_num_of_payments_l7d',
       'dcom_total_move_likely_specialist_l3d',
       'dcom_total_move_likely_specialist_l7d',
       'dcom_total_move_internal_search_l3d',
       'dcom_total_buyflow_customer_order_review_step',
       'dcom_total_scheduled_single_pay', 'dcom_total_itg_completes',
       'dcom_total_uid_username_password_creation', 'learntvpage_flag',
       'careerspage_flag', 'watchtvonlinepage_flag',
       'downloadnortonintsecpage_flag', 'manageaccountpage_flag',
       'upgradeservicepage_flag',
       'prev_video_tier_name_enc','target']]
oot201906 = oot201906.drop_duplicates()

oot201903 = oot201903[['dcom_web_num_of_visits_l7d', 'dcom_web_num_of_product_views_7d',
       'dcom_web_num_of_payments_15d', 'dcom_web_move_likely_specialist_l3d',
       'dcom_web_bill_pay', 'dcom_web_downgrade_order',
       'dcom_web_call_me_back', 'dcom_web_localization',
       'dcom_web_uid_personal_email_verified',
       'dcom_web_uid_account_lookup_complete',
       'dcom_mobile_timespent_seconds_l7d',
       'dcom_mobile_num_of_product_views_7d',
       'dcom_mobile_num_of_payments_15d', 'dcom_mobile_move_likely_l7d',
       'dcom_mobile_move_likely_specialist_l3d', 'dcom_mobile_buyflow_starts',
       'dcom_mobile_sik_eligible', 'dcom_mobile_bill_pay',
       'dcom_mobile_itg_completes', 'dcom_mobile_uid_password_reset_complete',
       'dcom_mobile_localization_decision', 'dcom_total_num_of_payments_l7d',
       'dcom_total_move_likely_specialist_l3d',
       'dcom_total_move_likely_specialist_l7d',
       'dcom_total_move_internal_search_l3d',
       'dcom_total_buyflow_customer_order_review_step',
       'dcom_total_scheduled_single_pay', 'dcom_total_itg_completes',
       'dcom_total_uid_username_password_creation', 'learntvpage_flag',
       'careerspage_flag', 'watchtvonlinepage_flag',
       'downloadnortonintsecpage_flag', 'manageaccountpage_flag',
       'upgradeservicepage_flag',
       'prev_video_tier_name_enc','target']]
oot201903 = oot201903.drop_duplicates()


# # XG Boost

# In[63]:


from xgboost import XGBClassifier
from matplotlib import pyplot


# In[64]:


model = XGBClassifier(base_score=0.5, booster='gbtree', colsample_bylevel=1,
              colsample_bynode=1, colsample_bytree=1, gamma=0,
              learning_rate=0.02, max_delta_step=0, max_depth=3,
              min_child_weight=1, missing=nan, n_estimators=400, n_jobs=1,
              nthread=None, random_state=0,
              reg_alpha=0, reg_lambda=1, scale_pos_weight=0.647322, seed=None,
              silent=None, subsample=1, verbosity=1)
#model_xg = model.fit(X_train[vif_cat_f['Var_name']], Y_train)
model_xg = model.fit(X_train[var_names],Y_train)
y_pred_train, y_proba_train = out_ks(X_train[var_names], Y_train, model_xg)
y_pred_test, y_proba_test = out_ks(X_test[var_names], Y_test, model_xg)

#y_pred_train, y_proba_train = out_ks(X_train[vif_cat_f['Var_name']], Y_train, model_xg)
#y_pred_test, y_proba_test = out_ks(X_test[vif_cat_f['Var_name']], Y_test, model_xg)
#model.fit(X_train, Y_train)


# In[169]:


class_names = [0,1]
acu_train, prec_train, rec_train, fs_train, cm_train = metrics(Y_train, y_pred_train, class_names, 'macro')
acu_test, prec_test, rec_test, fs_test, cm_test = metrics(Y_test, y_pred_test, class_names, 'macro')
# print ("AUC_Train : {0:.2%}".format(roc_train))
# print ("AUC_Test : {0:.2%}".format(roc_test))

print ("\n" + "Accuracy_Train : {0:.2%}".format(acu_train))
print ("Precision_Train : {0:.2%}".format(prec_train))
# print ("Recall_Train : {0:.2%}".format(rec_train))
# print ("FScore_Train : {0:.2%}".format(fs_train))
#print ("Confusion matrix: "+"\n",cm_train)

#print ("\n" + "Accuracy_Test : {0:.2%}".format(acu_test))
print ("Precision_Test : {0:.2%}".format(prec_test))
# print ("Recall_Test : {0:.2%}".format(rec_test))
# print ("FScore_Test : {0:.2%}".format(fs_test))
#print ("Confusion matrix: "+"\n",cm_test)
print ("\n" + "Accuracy_Test : {0:.2%}".format(acu_test))
plt.figure(figsize=(20, 4))
plt.subplot(1,4,1) 
plot_confusion_matrix(cm_train, class_names, normalize=True, title='Confusion matrix - Train', cmap=plt.cm.Blues)

plt.subplot(1,4,2)
plot_confusion_matrix(cm_test, class_names, normalize=True, title='Confusion matrix - Test', cmap=plt.cm.Reds)


# In[74]:


#Scoring on OOT201906
class_names = [0,1]
#y_pred_oot201906, y_proba_oot201906 = out_ks(oot201906[vif_cat_f['Var_name']], oot201906[target], model_xg)
y_pred_oot201906, y_proba_oot201906 = out_ks(oot201906[var_names], oot201906[target], model_xg)
acu_test, prec_test, rec_test, fs_test, cm_oot201906 = metrics(oot201906[target], y_pred_oot201906, class_names, 'macro')
print ("\n" + "Accuracy_Test : {0:.2%}".format(acu_test))
print ("Precision_Train : {0:.2%}".format(prec_test))
plot_confusion_matrix(cm_oot201906, class_names, normalize=True, title='Confusion matrix - Test', cmap=plt.cm.Reds)


# In[73]:


#Scoring on OOT201903
class_names = [0,1]
#y_pred_oot201903, y_proba_oot201903 = out_ks(oot201903[vif_cat_f['Var_name']], oot201903[target], model_xg)
y_pred_oot201903, y_proba_oot201903 = out_ks(oot201903[var_names], oot201903[target], model_xg)
acu_test, prec_test, rec_test, fs_test, cm_oot201903 = metrics(oot201903[target], y_pred_oot201903, class_names, 'macro')
print ("\n" + "Accuracy_Test : {0:.2%}".format(acu_test))
print ("Precision_Train : {0:.2%}".format(prec_test))
plot_confusion_matrix(cm_oot201903, class_names, normalize=True, title='Confusion matrix - Test', cmap=plt.cm.Reds)


# In[65]:


from eli5 import show_prediction
from eli5 import show_weights
show_weights(model_xg)


# In[66]:


from xgboost import plot_importance
import xgboost
ax = xgboost.plot_importance(model_xg)
thresholds = sort(model_xg.feature_importances_)
fig = ax.figure
fig.set_size_inches(10, 10)
pyplot.show()


# # Save Model Object as pickle

# In[67]:


import joblib
import pickle
# to save the model
joblib.dump(model_xg, open('/home/sbalas203/notebooks/vid_tier_upg.pkl', 'wb'))


# In[68]:


model_reloaded = joblib.load('/home/sbalas203/notebooks/vid_tier_upg.pkl')
model_reloaded


# # Model Interpretability

# In[69]:


import shap
explainer = shap.TreeExplainer(model_xg)
#shap_values_train = explainer.shap_values(X_train[vif_cat_f['Var_name']])
shap_values_train = explainer.shap_values(X_train[var_names])
# Shapley values for each data instance and feature
import seaborn as sns
#shap.summary_plot(shap_values_train, X_train[vif_cat_f['Var_name']],plot_type="bar")
shap.summary_plot(shap_values_train, X_train[var_names])


# In[75]:


final_data = pd.concat([oot201906,y_pred_oot201906], axis=1)
final_data[(final_data['Pred'] == 0.0) & (final_data['target'] == 1.0) & (final_data['prev_video_tier_name_enc'] != 2.0)]


# In[76]:


#final_data
final_data['Pred'].value_counts() 
          # (final_data['target'] ==1 )


# In[77]:


final_data['target'].value_counts()


# In[56]:


from eli5 import show_prediction
from eli5 import show_weights
from xgboost import XGBClassifier
from sklearn.feature_extraction import DictVectorizer
from sklearn.pipeline import make_pipeline
from sklearn.model_selection import cross_val_score

#Attempt to use SHAP on multi-class

#This line will not work for a multi-class model, so we comment out
#explainer = shap.TreeExplainer(mcl, model_output='probability', feature_dependence='independent', data=X)

explainer = shap.TreeExplainer(model_xg)
shap_values = explainer.shap_values(X_train[vif_cat_f['Var_name']])
#shap.initjs()
#for which_class in range(1,3):
display(shap.force_plot(explainer.expected_value[1], shap_values[1], X_train[vif_cat_f['Var_name']]))
    


# # Random Forest

# In[ ]:


clf = RandomForestClassifier(n_estimators=200, max_depth=3, max_features='sqrt', 
                             class_weight='balanced_subsample',n_jobs=-1, random_state =1, verbose=0)
#clf = RandomForestClassifier(n_estimators=60, criterion='gini', max_depth=5, min_samples_split=2, min_samples_leaf=2,
                            #max_features='auto', max_leaf_nodes=2, random_state=99)

clf_rf_u = clf.fit(X_train[redu['Var_name']], Y_train)
y_pred_train, y_proba_train = out_ks(X_train[redu['Var_name']], Y_train, clf_rf_u)
y_pred_test, y_proba_test = out_ks(X_test[redu['Var_name']], Y_test, clf_rf_u)


# In[103]:


class_names = [0,1]
acu_train, prec_train, rec_train, fs_train, cm_train = metrics(Y_train, y_pred_train, class_names, 'macro')
acu_test, prec_test, rec_test, fs_test, cm_test = metrics(Y_test, y_pred_test, class_names, 'macro')
# print ("AUC_Train : {0:.2%}".format(roc_train))
# print ("AUC_Test : {0:.2%}".format(roc_test))

print ("\n" + "Accuracy_Train : {0:.2%}".format(acu_train))
# print ("Precision_Train : {0:.2%}".format(prec_train))
# print ("Recall_Train : {0:.2%}".format(rec_train))
# print ("FScore_Train : {0:.2%}".format(fs_train))
#print ("Confusion matrix: "+"\n",cm_train)

print ("\n" + "Accuracy_Test : {0:.2%}".format(acu_test))
# print ("Precision_Test : {0:.2%}".format(prec_test))
# print ("Recall_Test : {0:.2%}".format(rec_test))
# print ("FScore_Test : {0:.2%}".format(fs_test))
#print ("Confusion matrix: "+"\n",cm_test)

plt.figure(figsize=(20, 4))
plt.subplot(1,4,1) 
plot_confusion_matrix(cm_train, class_names, normalize=True, title='Confusion matrix - Train', cmap=plt.cm.Blues)

plt.subplot(1,4,2)
plot_confusion_matrix(cm_test, class_names, normalize=True, title='Confusion matrix - Test', cmap=plt.cm.Reds)


# In[101]:


#Scoring on OOT201906
class_names = [0,1]
y_pred_oot201906, y_proba_oot201906 = out_ks(oot201906[redu['Var_name']], oot201906[target], clf_rf_u)
acu_test, prec_test, rec_test, fs_test, cm_oot201906 = metrics(oot201906[target], y_pred_oot201906, class_names, 'macro')
print ("\n" + "Accuracy_Test : {0:.2%}".format(acu_test))
plot_confusion_matrix(cm_oot201906, class_names, normalize=True, title='Confusion matrix - Test', cmap=plt.cm.Reds)


# In[102]:


#Scoring on OOT201903
class_names = [0,1]
y_pred_oot201903, y_proba_oot201903 = out_ks(oot201903[redu['Var_name']], oot201903[target], clf_rf_u)
acu_test, prec_test, rec_test, fs_test, cm_oot201903 = metrics(oot201903[target], y_pred_oot201903, class_names, 'macro')
print ("\n" + "Accuracy_Test : {0:.2%}".format(acu_test))
plot_confusion_matrix(cm_oot201903, class_names, normalize=True, title='Confusion matrix - Test', cmap=plt.cm.Reds)


# # MLP Classifier

# In[105]:


from sklearn.neural_network import MLPClassifier
classifier = MLPClassifier(hidden_layer_sizes=(100,100), activation='relu', 
              solver='adam', alpha=0.1, batch_size='auto', 
              learning_rate='constant', learning_rate_init=0.001, power_t=0.5,
              max_iter=1000, shuffle=True, random_state=1, tol=0.0001, verbose=False, 
              warm_start=False, momentum=0.9, nesterovs_momentum=True, early_stopping=False)


# In[107]:


mlp = classifier.fit(X_train[var_names],Y_train)

#mlp_u = mlp.fit(X_train[redu['Var_name']], Y_train)
#y_pred_train, y_proba_train = out_ks(X_train[redu['Var_name']], Y_train, mlp_u)
#y_pred_test, y_proba_test = out_ks(X_test[redu['Var_name']], Y_test, mlp_u)

mlp_u = mlp.fit(X_train[var_names],Y_train)
y_pred_train, y_proba_train = out_ks(X_train[var_names], Y_train, mlp_u)
y_pred_test, y_proba_test = out_ks(X_test[var_names], Y_test, mlp_u)


# In[108]:


acu_train, prec_train, rec_train, fs_train, cm_train = metrics(Y_train, y_pred_train, class_names, 'macro')
acu_test, prec_test, rec_test, fs_test, cm_test = metrics(Y_test, y_pred_test, class_names, 'macro')
print ("\n" + "Accuracy_Train : {0:.2%}".format(acu_train))
print ("\n" + "Accuracy_Test : {0:.2%}".format(acu_test))
plt.figure(figsize=(20, 4))
plt.subplot(1,4,1) 
plot_confusion_matrix(cm_train, class_names, normalize=True, title='Confusion matrix - Train', cmap=plt.cm.Blues)
plt.subplot(1,4,2)
plot_confusion_matrix(cm_test, class_names, normalize=True, title='Confusion matrix - Test', cmap=plt.cm.Reds)


# # Logistic Regression

# In[103]:


lgr = LogisticRegression(random_state=87)

clf_lr_f = lgr.fit(X_train[vif_cat_f['Var_name']], Y_train)

y_pred_train, y_proba_train = out_ks(X_train[vif_cat_f['Var_name']], Y_train, clf_lr_f)
y_pred_test, y_proba_test = out_ks(X_test[vif_cat_f['Var_name']], Y_test, clf_lr_f)


# In[104]:


acu_train, prec_train, rec_train, fs_train, cm_train = metrics(Y_train, y_pred_train, class_names, 'macro')
acu_test, prec_test, rec_test, fs_test, cm_test = metrics(Y_test, y_pred_test, class_names, 'macro')
# print ("AUC_Train : {0:.2%}".format(roc_train))
# print ("AUC_Test : {0:.2%}".format(roc_test))

print ("\n" + "Accuracy_Train : {0:.2%}".format(acu_train))

print ("\n" + "Accuracy_Test : {0:.2%}".format(acu_test))

plt.figure(figsize=(20, 4))
plt.subplot(1,4,1) 
plot_confusion_matrix(cm_train, class_names, normalize=True, title='Confusion matrix - Train', cmap=plt.cm.Blues)

plt.subplot(1,4,2)
plot_confusion_matrix(cm_test, class_names, normalize=True, title='Confusion matrix - Test', cmap=plt.cm.Reds)


# # Gradient Boosting

# In[105]:


gb = GradientBoostingClassifier(learning_rate=0.03, n_estimators=1000, 
min_samples_split=2, min_samples_leaf=2, 
max_depth=3, min_impurity_split=1e-09, random_state=87, max_features='auto', 
                                verbose=0)

clf_gb_f = gb.fit(X_train[vif_cat_f['Var_name']], Y_train)

y_pred_train, y_proba_train = out_ks(X_train[vif_cat_f['Var_name']], Y_train, clf_gb_f)
y_pred_test, y_proba_test = out_ks(X_test[vif_cat_f['Var_name']], Y_test, clf_gb_f)


# In[106]:


acu_train, prec_train, rec_train, fs_train, cm_train = metrics(Y_train, y_pred_train, class_names, 'macro')
acu_test, prec_test, rec_test, fs_test, cm_test = metrics(Y_test, y_pred_test, class_names, 'macro')

print ("\n" + "Accuracy_Train : {0:.2%}".format(acu_train))
print ("\n" + "Accuracy_Test : {0:.2%}".format(acu_test))
plt.figure(figsize=(20, 4))
plt.subplot(1,4,1) 
plot_confusion_matrix(cm_train, class_names, normalize=True, title='Confusion matrix - Train', cmap=plt.cm.Blues)

plt.subplot(1,4,2)
plot_confusion_matrix(cm_test, class_names, normalize=True, title='Confusion matrix - Test', cmap=plt.cm.Reds)


# # Genetic algorithm for feature selection

# In[59]:


from genetic_selection import GeneticSelectionCV


# In[60]:


clf = RandomForestClassifier(n_estimators=200, max_depth=3, max_features='sqrt', 
                             class_weight='balanced_subsample',n_jobs=-1, random_state =1, verbose=0)
selector = GeneticSelectionCV(clf,cv=3,verbose=1,scoring="accuracy",
                                  max_features=5,n_population=50,
                                  crossover_proba=0.5,mutation_proba=0.2, n_generations=40,
                                  crossover_independent_proba=0.5,
                                  mutation_independent_proba=0.05,tournament_size=3,n_gen_no_change=10,
                                  caching=True,n_jobs=-1)
selector = selector.fit(X_train[var_names],Y_train)
print(selector.support_)


# In[61]:


itemindex = np.where(selector.support_==True)
itemindex


# In[89]:


X_train.iloc[:,[4,12,19,  30,  46,  49,  50,  55,  61,  62,  73,  75,  78,
         80,  85,  91,  96,  99, 110, 117, 119, 130, 137, 138, 141, 147,
        152, 162, 164, 182, 191, 193, 196, 212, 218, 232, 233, 239, 244,
        253, 256, 283]].columns

