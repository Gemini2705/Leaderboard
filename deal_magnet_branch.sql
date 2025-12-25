deal_magnet_final AS (
    SELECT 
        COALESCE(dcm.state_name, dpm.state_name, dcq.state_name, dfy.state_name) as state_name,
        COALESCE(dcm.branch_name, dpm.branch_name, dcq.branch_name, dfy.branch_name) as branch_name,
        COALESCE(dcm.branch_code, dpm.branch_code, dcq.branch_code, dfy.branch_code) as branch_code,
        
        dcm.deal_magnet_current_month_rank as "Deal Magnet Current Month Rank",
        dpm.deal_magnet_previous_month_rank as "Deal Magnet Previous Month Rank",
        dcq.deal_magnet_current_quarter_rank as "Deal Magnet Current Quarter Rank",
        dfy.deal_magnet_current_fy_rank as "Deal Magnet Current FY Rank"
        
    FROM (
        SELECT state_name, branch_name, branch_code,
               RANK() OVER (ORDER BY monthly_applications DESC) as deal_magnet_current_month_rank
        FROM current_month_login WHERE monthly_applications > deal_magnet_final AS (
    SELECT 
        COALESCE(dcm.state_name, dpm.state_name, dcq.state_name, dfy.state_name) as state_name,
        COALESCE(dcm.branch_name, dpm.branch_name, dcq.branch_name, dfy.branch_name) as branch_name,
        COALESCE(dcm.branch_code, dpm.branch_code, dcq.branch_code, dfy.branch_code) as branch_code,
        
        dcm.deal_magnet_current_month_rank as "Deal Magnet Current Month Rank",
        dpm.deal_magnet_previous_month_rank as "Deal Magnet Previous Month Rank",
        dcq.deal_magnet_current_quarter_rank as "Deal Magnet Current Quarter Rank",
        dfy.deal_magnet_current_fy_rank as "Deal Magnet Current FY Rank"
        
    FROM (
        SELECT cml.state_name, cml.branch_name, cml.branch_code,
               RANK() OVER (ORDER BY cml.monthly_applications DESC, cmd.monthly_disbursal_amount DESC, cmd.monthly_disbursals DESC) as deal_magnet_current_month_rank
        FROM current_month_login cml
        LEFT JOIN current_month_data cmd ON cml.state_name = cmd.state_name 
            AND cml.branch_name = cmd.branch_name 
            AND cml.branch_code = cmd.branch_code
        WHERE cml.monthly_applications > 0
    ) dcm
    FULL OUTER JOIN (
        SELECT pml.state_name, pml.branch_name, pml.branch_code,
               RANK() OVER (ORDER BY pml.monthly_applications DESC, pmd.monthly_disbursal_amount DESC, pmd.monthly_disbursals DESC) as deal_magnet_previous_month_rank
        FROM previous_month_login pml
        LEFT JOIN previous_month_data pmd ON pml.state_name = pmd.state_name 
            AND pml.branch_name = pmd.branch_name 
            AND pml.branch_code = pmd.branch_code
        WHERE pml.monthly_applications > 0
    ) dpm ON dcm.state_name = dpm.state_name AND dcm.branch_name = dpm.branch_name AND dcm.branch_code = dpm.branch_code
    FULL OUTER JOIN (
        SELECT cql.state_name, cql.branch_name, cql.branch_code,
               RANK() OVER (ORDER BY cql.quarterly_applications DESC, cqd.quarterly_disbursal_amount DESC, cqd.quarterly_disbursals DESC) as deal_magnet_current_quarter_rank
        FROM current_quarter_login cql
        LEFT JOIN current_quarter_data cqd ON cql.state_name = cqd.state_name 
            AND cql.branch_name = cqd.branch_name 
            AND cql.branch_code = cqd.branch_code
        WHERE cql.quarterly_applications > 0
    ) dcq ON COALESCE(dcm.state_name, dpm.state_name) = dcq.state_name
        AND COALESCE(dcm.branch_name, dpm.branch_name) = dcq.branch_name
        AND COALESCE(dcm.branch_code, dpm.branch_code) = dcq.branch_code
    FULL OUTER JOIN (
        SELECT cfl.state_name, cfl.branch_name, cfl.branch_code,
               RANK() OVER (ORDER BY cfl.yearly_applications DESC, cfd.yearly_disbursal_amount DESC, cfd.yearly_disbursals DESC) as deal_magnet_current_fy_rank
        FROM current_fy_login cfl
        LEFT JOIN current_fy_data cfd ON cfl.state_name = cfd.state_name 
            AND cfl.branch_name = cfd.branch_name 
            AND cfl.branch_code = cfd.branch_code
        WHERE cfl.yearly_applications > 0
    ) dfy ON COALESCE(dcm.state_name, dpm.state_name, dcq.state_name) = dfy.state_name
        AND COALESCE(dcm.branch_name, dpm.branch_name, dcq.branch_name) = dfy.branch_name
        AND COALESCE(dcm.branch_code, dpm.branch_code, dcq.branch_code) = dfy.branch_code
)
SELECT * FROM deal_magnet_final ORDER BY "Deal Magnet Current Month Rank" NULLS LAST
    ) dcm
    FULL OUTER JOIN (
        SELECT state_name, branch_name, branch_code,
               RANK() OVER (ORDER BY monthly_applications DESC) as deal_magnet_previous_month_rank
        FROM previous_month_login WHERE monthly_applications > 0
    ) dpm ON dcm.state_name = dpm.state_name AND dcm.branch_name = dpm.branch_name AND dcm.branch_code = dpm.branch_code
    FULL OUTER JOIN (
        SELECT state_name, branch_name, branch_code,
               RANK() OVER (ORDER BY quarterly_applications DESC) as deal_magnet_current_quarter_rank
        FROM current_quarter_login WHERE quarterly_applications > 0
    ) dcq ON COALESCE(dcm.state_name, dpm.state_name) = dcq.state_name
        AND COALESCE(dcm.branch_name, dpm.branch_name) = dcq.branch_name
        AND COALESCE(dcm.branch_code, dpm.branch_code) = dcq.branch_code
    FULL OUTER JOIN (
        SELECT state_name, branch_name, branch_code,
               RANK() OVER (ORDER BY yearly_applications DESC) as deal_magnet_current_fy_rank
        FROM current_fy_login WHERE yearly_applications > 0
    ) dfy ON COALESCE(dcm.state_name, dpm.state_name, dcq.state_name) = dfy.state_name
        AND COALESCE(dcm.branch_name, dpm.branch_name, dcq.branch_name) = dfy.branch_name
        AND COALESCE(dcm.branch_code, dpm.branch_code, dcq.branch_code) = dfy.branch_code
)

SELECT * FROM deal_magnet_final ORDER BY "Deal Magnet Current Month Rank" NULLS LAST
