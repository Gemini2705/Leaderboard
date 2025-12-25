rainmaker_final AS (
    SELECT 
        COALESCE(rcm.state_name, rpm.state_name, rcq.state_name, rfy.state_name) as state_name,
        COALESCE(rcm.branch_name, rpm.branch_name, rcq.branch_name, rfy.branch_name) as branch_name,
        COALESCE(rcm.branch_code, rpm.branch_code, rcq.branch_code, rfy.branch_code) as branch_code,
        COALESCE(rcm.sourcing_rm_name, rpm.sourcing_rm_name, rcq.sourcing_rm_name, rfy.sourcing_rm_name) as sourcing_rm_name,
        COALESCE(rcm.sourcing_rm_code, rpm.sourcing_rm_code, rcq.sourcing_rm_code, rfy.sourcing_rm_code) as sourcing_rm_code,
        
        -- Rainmaker Ranks Only
        rcm.rainmaker_current_month_rank as "Rainmaker Current Month Rank",
        rpm.rainmaker_previous_month_rank as "Rainmaker Previous Month Rank",
        rcq.rainmaker_current_quarter_rank as "Rainmaker Current Quarter Rank",
        rfy.rainmaker_current_fy_rank as "Rainmaker Current FY Rank"
        
    FROM (
        SELECT state_name, branch_name, branch_code, sourcing_rm_name, sourcing_rm_code,
               RANK() OVER (ORDER BY monthly_disbursal_amount DESC, monthly_disbursals DESC) as rainmaker_current_month_rank
        FROM current_month_data WHERE monthly_disbursal_amount > 0
    ) rcm
    FULL OUTER JOIN (
        SELECT state_name, branch_name, branch_code, sourcing_rm_name, sourcing_rm_code,
               RANK() OVER (ORDER BY monthly_disbursal_amount DESC, monthly_disbursals DESC) as rainmaker_previous_month_rank
        FROM previous_month_data WHERE monthly_disbursal_amount > 0
    ) rpm ON rcm.state_name = rpm.state_name 
        AND rcm.branch_name = rpm.branch_name 
        AND rcm.branch_code = rpm.branch_code
        AND rcm.sourcing_rm_name = rpm.sourcing_rm_name
        AND rcm.sourcing_rm_code = rpm.sourcing_rm_code
    FULL OUTER JOIN (
        SELECT state_name, branch_name, branch_code, sourcing_rm_name, sourcing_rm_code,
               RANK() OVER (ORDER BY quarterly_disbursal_amount DESC, quarterly_disbursals DESC) as rainmaker_current_quarter_rank
        FROM current_quarter_data WHERE quarterly_disbursal_amount > 0
    ) rcq ON COALESCE(rcm.state_name, rpm.state_name) = rcq.state_name
        AND COALESCE(rcm.branch_name, rpm.branch_name) = rcq.branch_name  
        AND COALESCE(rcm.branch_code, rpm.branch_code) = rcq.branch_code
        AND COALESCE(rcm.sourcing_rm_name, rpm.sourcing_rm_name) = rcq.sourcing_rm_name
        AND COALESCE(rcm.sourcing_rm_code, rpm.sourcing_rm_code) = rcq.sourcing_rm_code
    FULL OUTER JOIN (
        SELECT state_name, branch_name, branch_code, sourcing_rm_name, sourcing_rm_code,
               RANK() OVER (ORDER BY yearly_disbursal_amount DESC, yearly_disbursals DESC) as rainmaker_current_fy_rank
        FROM current_fy_data WHERE yearly_disbursal_amount > 0
    ) rfy ON COALESCE(rcm.state_name, rpm.state_name, rcq.state_name) = rfy.state_name
        AND COALESCE(rcm.branch_name, rpm.branch_name, rcq.branch_name) = rfy.branch_name
        AND COALESCE(rcm.branch_code, rpm.branch_code, rcq.branch_code) = rfy.branch_code
        AND COALESCE(rcm.sourcing_rm_name, rpm.sourcing_rm_name, rcq.sourcing_rm_name) = rfy.sourcing_rm_name
        AND COALESCE(rcm.sourcing_rm_code, rpm.sourcing_rm_code, rcq.sourcing_rm_code) = rfy.sourcing_rm_code
)
SELECT * FROM rainmaker_final ORDER BY "Rainmaker Current Month Rank" NULLS LAST
