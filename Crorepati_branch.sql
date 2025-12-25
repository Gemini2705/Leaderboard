crorepati_final AS (
    SELECT 
        COALESCE(ccm.state_name, cpm.state_name, ccq.state_name, cfy.state_name) as state_name,
        COALESCE(ccm.branch_name, cpm.branch_name, ccq.branch_name, cfy.branch_name) as branch_name,
        COALESCE(ccm.branch_code, cpm.branch_code, ccq.branch_code, cfy.branch_code) as branch_code,
        
        ccm.crorepati_current_month_rank as "Crorepati Current Month Rank",
        cpm.crorepati_previous_month_rank as "Crorepati Previous Month Rank", 
        ccq.crorepati_current_quarter_rank as "Crorepati Current Quarter Rank",
        cfy.crorepati_current_fy_rank as "Crorepati Current FY Rank"
        
    FROM (
        SELECT state_name, branch_name, branch_code,
               RANK() OVER (ORDER BY monthly_disbursal_amount DESC) as crorepati_current_month_rank
        FROM current_month_data WHERE monthly_disbursal_amount >= 10000000  
    ) ccm
    FULL OUTER JOIN (
        SELECT state_name, branch_name, branch_code,
               RANK() OVER (ORDER BY monthly_disbursal_amount DESC) as crorepati_previous_month_rank
        FROM previous_month_data WHERE monthly_disbursal_amount >= 10000000  
    ) cpm ON ccm.state_name = cpm.state_name AND ccm.branch_name = cpm.branch_name AND ccm.branch_code = cpm.branch_code
    FULL OUTER JOIN (
        SELECT state_name, branch_name, branch_code,
               RANK() OVER (ORDER BY quarterly_disbursal_amount DESC) as crorepati_current_quarter_rank
        FROM current_quarter_data WHERE quarterly_disbursal_amount >= 30000000  
    ) ccq ON COALESCE(ccm.state_name, cpm.state_name) = ccq.state_name 
        AND COALESCE(ccm.branch_name, cpm.branch_name) = ccq.branch_name 
        AND COALESCE(ccm.branch_code, cpm.branch_code) = ccq.branch_code
    FULL OUTER JOIN (
        SELECT state_name, branch_name, branch_code,
               RANK() OVER (ORDER BY yearly_disbursal_amount DESC) as crorepati_current_fy_rank
        FROM current_fy_data WHERE yearly_disbursal_amount >= 120000000  
    ) cfy ON COALESCE(ccm.state_name, cpm.state_name, ccq.state_name) = cfy.state_name 
        AND COALESCE(ccm.branch_name, cpm.branch_name, ccq.branch_name) = cfy.branch_name 
        AND COALESCE(ccm.branch_code, cpm.branch_code, ccq.branch_code) = cfy.branch_code
)

SELECT * FROM crorepati_final ORDER BY "Crorepati Current Month Rank" NULLS LAST
