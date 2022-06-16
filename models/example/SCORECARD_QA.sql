
{{
    config (materialized="table")
}}

select
    o."tspc_opportunity_c"             as SFDC_OPP_ID,
    o."tspc_scorecardscoreratio_c"     as Opp_Score,
    s."name"                           as section,
    s."tspc_totalscoreratio_c"         as section_score,
    q."name"                           as question,
    q."tspc_isanswered_c"                 Is_answered,
    --q."tspc_maxscore_c" as question_max_score,
    --q."tspc_score_c" as question_score,
    case
        when q."tspc_mode_c" = 'Text' then q."tspc_textanswerfull_c"
        when q."tspc_mode_c" = 'Yes-No' then q."tspc_answer_c"
        when tq."tspc_field_c" = 'Champion_LU__c' then ifnull(ch."name", oo."champion_c")
        when tq."tspc_field_c" = 'Champion_Status__c' then oo."champion_status_c"
        when tq."tspc_field_c" = 'Competitor__c' then "competitor_c"
        when tq."tspc_field_c" = 'Decision_Criteria__c' then oo."decision_criteria_c"
        when tq."tspc_field_c" = 'Decision_Process__c' then oo."decision_process_c"
        when tq."tspc_field_c" = 'Decisions_Criteria_Status__c' then oo."decisions_criteria_status_c"
        when tq."tspc_field_c" = 'Decisions_Process_Status__c' then oo."decisions_process_status_c"
        when tq."tspc_field_c" = 'Economic_Buyer_LU__c' then ifnull(eb."name", oo."economic_buyer_c")
        when tq."tspc_field_c" = 'Economic_Buyer_Status__c' then oo."economic_buyer_status_c"
        when tq."tspc_field_c" = 'Identified_Pain_Status__c' then oo."identified_pain_status_c"
        when tq."tspc_field_c" = 'Identified_Pain__c' then oo."identified_pain_c"
        when tq."tspc_field_c" = 'Metrics_Status__c' then oo."metrics_status_c"
        when tq."tspc_field_c" = 'Metrics__c' then oo."metrics_c"
        when tq."tspc_field_c" = 'Paper_Process_Status__c' then oo."paper_process_status_c"
        when tq."tspc_field_c" = 'Paper_Process__c' then oo."paper_process_c"
        else an."name" end             as answer,
    o."tspc_opportunity_c" || q."id" || ifnull(answer,'') as key,
    q."lastmodifieddate"               as question_last_modified_date,
    current_timestamp() as last_update
    --q."tspc_mode_c",
    --tq."tspc_field_c"
    --ah.*
from (
         select *, row_number() over (partition by "tspc_opportunity_c" order by "createddate" desc) as rn
         from ALOOMA_EVENTS.PUBLIC.DV_PAI_SF_TSPC_DEAL
     ) o
         left join ALOOMA_EVENTS.PUBLIC.DV_PAI_SF_TSPC_DEAL_QUESTION_CATEGORY s on s."tspc_deal_c" = o."id"
         left join ALOOMA_EVENTS."PUBLIC".DV_PAI_SF_TSPC_DEAL_QUESTION q
                   on o."id" = q."tspc_deal_c" and q."tspc_questioncategory_c" = s."id"
         left join ALOOMA_EVENTS."PUBLIC".DV_PAI_SF_TSPC_DEAL_QUESTION_ANSWER an
                   on an."tspc_question_c" = q."id" and ifnull(an."isdeleted", false) = false
                       and ifnull(an."tspc_isselected_c", true) = true
         left join ALOOMA_EVENTS.PUBLIC.DV_SF_TSPC__TEMPLATEQUESTION__C tq on q."tspc_templatequestion_c" = tq."id"
         left join ALOOMA_EVENTS.PUBLIC.DV_OPPORTUNITY_ALL oo on o."tspc_opportunity_c" = oo."id" and oo."load_date" =
                                                                                                      (select max("load_date")
                                                                                                       from ALOOMA_EVENTS.PUBLIC.DV_OPPORTUNITY_ALL)
         left join ALOOMA_EVENTS.PUBLIC.DV_SF_CONTACT ch
                   on ch."id" = oo."champion_lu_c" and ch."load_date" = (select max("load_date")
                                                                         from ALOOMA_EVENTS.PUBLIC.DV_SF_CONTACT)
         left join ALOOMA_EVENTS.PUBLIC.DV_SF_CONTACT eb
                   on eb."id" = oo."economic_buyer_lu_c" and eb."load_date" = (select max("load_date")
                                                                               from ALOOMA_EVENTS.PUBLIC.DV_SF_CONTACT)
--left join ALOOMA_EVENTS.PUBLIC.DV_PAI_SF_TSPC_DEAL_QUESTION_ANSWER _HISTORY ah on ah."parentid"=an."id"

where o.rn = 1
      --and o."tspc_opportunity_c" = '0061K00000j47KuQAI'
      --and Is_answered and answer is null
order by 3, 6
