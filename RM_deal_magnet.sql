deal_magnet_final AS (
    SELECT 
        COALESCE(dcm.state_name, dpm.state_name, dcq.state_name, dfy.state_name) as state_name,
        COALESCE(dcm.branch_name, dpm.branch_name, dcq.branch_name, dfy.branch_name) as branch_name,
        COALESCE(dcm.branch_code, dpm.branch_code, dcq.branch_code, dfy.branch_code) as branch_code,
        COALESCE(dcm.sourcing_rm_name, dpm.sourcing_rm_name, dcq.sourcing_rm_name, dfy.sourcing_rm_name) as sourcing_rm_name,
        COALESCE(dcm.sourcing_rm_code, dpm.sourcing_rm_code, dcq.sourcing_rm_code, dfy.sourcing_rm_code) as sourcing_rm_code,
        
        -- Deal Magnet Ranks Only
        dcm.deal_magnet_current_month_rank as "Deal Magnet Current Month Rank",
        dpm.deal_magnet_previous_month_rank as "Deal Magnet Previous Month Rank",
        dcq.deal_magnet_current_quarter_rank as "Deal Magnet Current Quarter Rank",
        dfy.deal_magnet_current_fy_rank as "Deal Magnet Current FY Rank"
        
    FROM (
        SELECT cml.state_name, cml.branch_name, cml.branch_code, cml.sourcing_rm_name, cml.sourcing_rm_code,
               RANK() OVER (ORDER BY cml.monthly_applications DESC, cmd.monthly_disbursal_amount DESC, cmd.monthly_disbursals DESC) as deal_magnet_current_month_rank
        FROM current_month_login cml
        LEFT JOIN current_month_data cmd ON cml.state_name = cmd.state_name 
            AND cml.branch_name = cmd.branch_name 
            AND cml.branch_code = cmd.branch_code 
            AND cml.sourcing_rm_name = cmd.sourcing_rm_name 
            AND cml.sourcing_rm_code = cmd.sourcing_rm_code
        WHERE cml.monthly_applications > 0
    ) dcm
    FULL OUTER JOIN (
        SELECT pml.state_name, pml.branch_name, pml.branch_code, pml.sourcing_rm_name, pml.sourcing_rm_code,
               RANK() OVER (ORDER BY pml.monthly_applications DESC, pmd.monthly_disbursal_amount DESC, pmd.monthly_disbursals DESC) as deal_magnet_previous_month_rank
        FROM previous_month_login pml
        LEFT JOIN previous_month_data pmd ON pml.state_name = pmd.state_name 
            AND pml.branch_name = pmd.branch_name 
            AND pml.branch_code = pmd.branch_code 
            AND pml.sourcing_rm_name = pmd.sourcing_rm_name 
            AND pml.sourcing_rm_code = pmd.sourcing_rm_code
        WHERE pml.monthly_applications > 0
    ) dpm ON dcm.state_name = dpm.state_name 
        AND dcm.branch_name = dpm.branch_name 
        AND dcm.branch_code = dpm.branch_code
        AND dcm.sourcing_rm_name = dpm.sourcing_rm_name
        AND dcm.sourcing_rm_code = dpm.sourcing_rm_code
    FULL OUTER JOIN (
        SELECT cql.state_name, cql.branch_name, cql.branch_code, cql.sourcing_rm_name, cql.sourcing_rm_code,
               RANK() OVER (ORDER BY cql.quarterly_applications DESC, cqd.quarterly_disbursal_amount DESC, cqd.quarterly_disbursals DESC) as deal_magnet_current_quarter_rank
        FROM current_quarter_login cql
        LEFT JOIN current_quarter_data cqd ON cql.state_name = cqd.state_name 
            AND cql.branch_name = cqd.branch_name 
            AND cql.branch_code = cqd.branch_code 
            AND cql.sourcing_rm_name = cqd.sourcing_rm_name 
            AND cql.sourcing_rm_code = cqd.sourcing_rm_code
        WHERE cql.quarterly_applications > 0
    ) dcq ON COALESCE(dcm.state_name, dpm.state_name) = dcq.state_name
        AND COALESCE(dcm.branch_name, dpm.branch_name) = dcq.branch_name
        AND COALESCE(dcm.branch_code, dpm.branch_code) = dcq.branch_code
        AND COALESCE(dcm.sourcing_rm_name, dpm.sourcing_rm_name) = dcq.sourcing_rm_name
        AND COALESCE(dcm.sourcing_rm_code, dpm.sourcing_rm_code) = dcq.sourcing_rm_code
    FULL OUTER JOIN (
        SELECT cfl.state_name, cfl.branch_name, cfl.branch_code, cfl.sourcing_rm_name, cfl.sourcing_rm_code,
               RANK() OVER (ORDER BY cfl.yearly_applications DESC, cfd.yearly_disbursal_amount DESC, cfd.yearly_disbursals DESC) as deal_magnet_current_fy_rank
        FROM current_fy_login cfl
        LEFT JOIN current_fy_data cfd ON cfl.state_name = cfd.state_name 
            AND cfl.branch_name = cfd.branch_name 
            AND cfl.branch_code = cfd.branch_code 
            AND cfl.sourcing_rm_name = cfd.sourcing_rm_name 
            AND cfl.sourcing_rm_code = cfd.sourcing_rm_code
        WHERE cfl.yearly_applications > 0
    ) dfy ON COALESCE(dcm.state_name, dpm.state_name, dcq.state_name) = dfy.state_name
        AND COALESCE(dcm.branch_name, dpm.branch_name, dcq.branch_name) = dfy.branch_name
        AND COALESCE(dcm.branch_code, dpm.branch_code, dcq.branch_code) = dfy.branch_code
        AND COALESCE(dcm.sourcing_rm_name, dpm.sourcing_rm_name, dcq.sourcing_rm_name) = dfy.sourcing_rm_name
        AND COALESCE(dcm.sourcing_rm_code, dpm.sourcing_rm_code, dcq.sourcing_rm_code) = dfy.sourcing_rm_code
)
SELECT * FROM deal_magnet_final ORDER BY "Deal Magnet Current Month Rank" NULLS LAST
