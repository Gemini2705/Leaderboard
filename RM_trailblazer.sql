current_month_trailblazer AS (
    SELECT 
        dc.current_month_name,
        dc.financial_year,
        rm_best.state_name,
        rm_best.branch_name,
        rm_best.branch_code,
        rm_best.sourcing_rm_name,
        rm_best.sourcing_rm_code,
        rm_best.best_monthly_amount,
        rm_best.best_month_period,
        RANK() OVER (ORDER BY rm_best.best_monthly_amount DESC) as current_month_rank
    FROM (
        SELECT 
            md.state_name,
            md.branch_name,
            md.branch_code,
            md.sourcing_rm_name,
            md.sourcing_rm_code,
            MAX(md.monthly_disbursal_amount) as best_monthly_amount,
            (ARRAY_AGG(md.disb_month || ' ' || md.disb_year::text ORDER BY md.monthly_disbursal_amount DESC))[1] as best_month_period
        FROM monthly_data md
        CROSS JOIN date_calculations dc
        WHERE md.disb_financial_year = dc.financial_year
            AND (md.disb_year < dc.current_year OR 
                (md.disb_year = dc.current_year AND md.disb_month_num <= dc.current_month_num))
        GROUP BY md.state_name, md.branch_name, md.branch_code, md.sourcing_rm_name, md.sourcing_rm_code
    ) rm_best
    CROSS JOIN date_calculations dc
),

-- Previous Month Trailblazer: Best monthly performance in current FY till previous month (excluding current month)
previous_month_trailblazer AS (
    SELECT 
        dc.previous_month_name,
        dc.financial_year,
        rm_best.state_name,
        rm_best.branch_name,
        rm_best.branch_code,
        rm_best.sourcing_rm_name,
        rm_best.sourcing_rm_code,
        rm_best.best_monthly_amount,
        rm_best.best_month_period,
        RANK() OVER (ORDER BY rm_best.best_monthly_amount DESC) as previous_month_rank
    FROM (
        SELECT 
            md.state_name,
            md.branch_name,
            md.branch_code,
            md.sourcing_rm_name,
            md.sourcing_rm_code,
            MAX(md.monthly_disbursal_amount) as best_monthly_amount,
            (ARRAY_AGG(md.disb_month || ' ' || md.disb_year::text ORDER BY md.monthly_disbursal_amount DESC))[1] as best_month_period
        FROM monthly_data md
        CROSS JOIN date_calculations dc
        WHERE md.disb_financial_year = dc.financial_year
            AND (md.disb_year < dc.current_year OR 
                (md.disb_year = dc.current_year AND md.disb_month_num < dc.current_month_num))
        GROUP BY md.state_name, md.branch_name, md.branch_code, md.sourcing_rm_name, md.sourcing_rm_code
        HAVING MAX(md.monthly_disbursal_amount) > 0
    ) rm_best
    CROSS JOIN date_calculations dc
),

-- Current Quarter Trailblazer: Best quarterly performance in current FY till current quarter
current_quarter_trailblazer AS (
    SELECT 
        dc.current_quarter,
        dc.financial_year,
        rm_best.state_name,
        rm_best.branch_name,
        rm_best.branch_code,
        rm_best.sourcing_rm_name,
        rm_best.sourcing_rm_code,
        rm_best.best_quarterly_amount,
        rm_best.best_quarter_period,
        RANK() OVER (ORDER BY rm_best.best_quarterly_amount DESC) as current_quarter_rank
    FROM (
        SELECT 
            qd.state_name,
            qd.branch_name,
            qd.branch_code,
            qd.sourcing_rm_name,
            qd.sourcing_rm_code,
            MAX(qd.quarterly_disbursal_amount) as best_quarterly_amount,
            (ARRAY_AGG(qd.disb_quarter || ' FY' || qd.disb_financial_year::text ORDER BY qd.quarterly_disbursal_amount DESC))[1] as best_quarter_period
        FROM quarterly_data qd
        CROSS JOIN date_calculations dc
        WHERE qd.disb_financial_year = dc.financial_year
            AND (
                (dc.current_quarter = 'Q1' AND qd.disb_quarter = 'Q1') OR
                (dc.current_quarter = 'Q2' AND qd.disb_quarter IN ('Q1', 'Q2')) OR
                (dc.current_quarter = 'Q3' AND qd.disb_quarter IN ('Q1', 'Q2', 'Q3')) OR
                (dc.current_quarter = 'Q4' AND qd.disb_quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))
            )
        GROUP BY qd.state_name, qd.branch_name, qd.branch_code, qd.sourcing_rm_name, qd.sourcing_rm_code
    ) rm_best
    CROSS JOIN date_calculations dc
),

-- Current Financial Year Trailblazer: Total yearly performance in current FY till current month
current_fy_trailblazer AS (
    SELECT 
        dc.financial_year,
        yd.state_name,
        yd.branch_name,
        yd.branch_code,
        yd.sourcing_rm_name,
        yd.sourcing_rm_code,
        yd.yearly_disbursal_amount as fy_amount,
        'FY' || yd.disb_financial_year::text as fy_period,
        RANK() OVER (ORDER BY yd.yearly_disbursal_amount DESC) as current_fy_rank
    FROM yearly_data yd
    CROSS JOIN date_calculations dc
    WHERE yd.disb_financial_year = dc.financial_year
)

-- Final trailblazer ranking combining all categories - RM Level
SELECT 
    COALESCE(cmt.state_name, pmt.state_name, cqt.state_name, cft.state_name) as state_name,
    COALESCE(cmt.branch_name, pmt.branch_name, cqt.branch_name, cft.branch_name) as branch_name,
    COALESCE(cmt.branch_code, pmt.branch_code, cqt.branch_code, cft.branch_code) as branch_code,
    COALESCE(cmt.sourcing_rm_name, pmt.sourcing_rm_name, cqt.sourcing_rm_name, cft.sourcing_rm_name) as sourcing_rm_name,
    COALESCE(cmt.sourcing_rm_code, pmt.sourcing_rm_code, cqt.sourcing_rm_code, cft.sourcing_rm_code) as sourcing_rm_code,
    
    -- Current Month Category
    cmt.current_month_rank as "Current Month Rank",
    cmt.best_month_period as "Current Month Achievement Period",
    cmt.best_monthly_amount as "Current Month Amount",
    
    -- Previous Month Category  
    pmt.previous_month_rank as "Previous Month Rank",
    pmt.best_month_period as "Previous Month Achievement Period",
    pmt.best_monthly_amount as "Previous Month Amount",
    
    -- Current Quarter Category
    cqt.current_quarter_rank as "Current Quarter Rank", 
    cqt.best_quarter_period as "Current Quarter Achievement Period",
    cqt.best_quarterly_amount as "Current Quarter Amount",
    
    -- Current Financial Year Category
    cft.current_fy_rank as "Current FY Rank",
    cft.fy_period as "Current FY Achievement Period", 
    cft.fy_amount as "Current FY Amount"
    
FROM current_month_trailblazer cmt
FULL OUTER JOIN previous_month_trailblazer pmt 
    ON cmt.state_name = pmt.state_name 
    AND cmt.branch_name = pmt.branch_name 
    AND cmt.branch_code = pmt.branch_code
    AND cmt.sourcing_rm_name = pmt.sourcing_rm_name
    AND cmt.sourcing_rm_code = pmt.sourcing_rm_code
FULL OUTER JOIN current_quarter_trailblazer cqt
    ON COALESCE(cmt.state_name, pmt.state_name) = cqt.state_name 
    AND COALESCE(cmt.branch_name, pmt.branch_name) = cqt.branch_name 
    AND COALESCE(cmt.branch_code, pmt.branch_code) = cqt.branch_code
    AND COALESCE(cmt.sourcing_rm_name, pmt.sourcing_rm_name) = cqt.sourcing_rm_name
    AND COALESCE(cmt.sourcing_rm_code, pmt.sourcing_rm_code) = cqt.sourcing_rm_code
FULL OUTER JOIN current_fy_trailblazer cft
    ON COALESCE(cmt.state_name, pmt.state_name, cqt.state_name) = cft.state_name 
    AND COALESCE(cmt.branch_name, pmt.branch_name, cqt.branch_name) = cft.branch_name 
    AND COALESCE(cmt.branch_code, pmt.branch_code, cqt.branch_code) = cft.branch_code
    AND COALESCE(cmt.sourcing_rm_name, pmt.sourcing_rm_name, cqt.sourcing_rm_name) = cft.sourcing_rm_name
    AND COALESCE(cmt.sourcing_rm_code, pmt.sourcing_rm_code, cqt.sourcing_rm_code) = cft.sourcing_rm_code
ORDER BY 
    COALESCE(cmt.current_month_rank, 9999),
    COALESCE(pmt.previous_month_rank, 9999),
    COALESCE(cqt.current_quarter_rank, 9999),
    COALESCE(cft.current_fy_rank, 9999)
