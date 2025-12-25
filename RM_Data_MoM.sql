WITH date_calculations AS (
    SELECT 
        CURRENT_DATE as current_date,
        EXTRACT(MONTH FROM CURRENT_DATE) as current_month_num,
        EXTRACT(YEAR FROM CURRENT_DATE) as current_year,
        to_char(CURRENT_DATE, 'Mon') as current_month_name,
        to_char(CURRENT_DATE - INTERVAL '1 month', 'Mon') as previous_month_name,
        EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month') as previous_month_num,
        EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 month') as previous_month_year,
        -- Financial year starts from April
        CASE 
            WHEN EXTRACT(MONTH FROM CURRENT_DATE) >= 4 
            THEN EXTRACT(YEAR FROM CURRENT_DATE)
            ELSE EXTRACT(YEAR FROM CURRENT_DATE) - 1 
        END as financial_year,
        -- Current quarter calculation (Apr-Jun=Q1, Jul-Sep=Q2, Oct-Dec=Q3, Jan-Mar=Q4)
        CASE 
            WHEN EXTRACT(MONTH FROM CURRENT_DATE) IN (4,5,6) THEN 'Q1'
            WHEN EXTRACT(MONTH FROM CURRENT_DATE) IN (7,8,9) THEN 'Q2'
            WHEN EXTRACT(MONTH FROM CURRENT_DATE) IN (10,11,12) THEN 'Q3'
            ELSE 'Q4'
        END as current_quarter
),

disbursal_base AS (
       SELECT 
        dd."Application Number" as application_number,
        coalesce(dd."Loan Account Number",ai."Loan Account No") as loan_account_number,
        ai."Customer Name" as customer_name,
        dd."Disbursal Date" as disbursal_date_ts,
        dd."First Disbursal Date" as first_disbursal_date_ts,
        coalesce(dd."Initiation By User Name",ai."Credit Officer Name") as co_code,
        round(dd."Disbursal Amount", 0) as disbursal_amount,
        round((dd."Disbursal Amount" + dd."Adjustment Amount"), 0) as actual_disbursal_amount,
		TO_CHAR(DATE(dd."Disbursal Date"), 'Mon') AS disb_month,
        to_char(date(dd."First Disbursal Date"), 'Mon') as first_disb_month,

		EXTRACT(MONTH FROM dd."Disbursal Date") as disb_month_num,
        EXTRACT(YEAR FROM dd."Disbursal Date") as disb_year,

		CASE 
            WHEN EXTRACT(MONTH FROM dd."Disbursal Date") >= 4 
            THEN EXTRACT(YEAR FROM dd."Disbursal Date")
            ELSE EXTRACT(YEAR FROM dd."Disbursal Date") - 1 
        END as disb_financial_year,
        -- Quarter for each disbursal
        CASE 
            WHEN EXTRACT(MONTH FROM dd."Disbursal Date") IN (4,5,6) THEN 'Q1'
            WHEN EXTRACT(MONTH FROM dd."Disbursal Date") IN (7,8,9) THEN 'Q2'
            WHEN EXTRACT(MONTH FROM dd."Disbursal Date") IN (10,11,12) THEN 'Q3'
            ELSE 'Q4'
        END as disb_quarter,
		
        row_number() over (partition by dd."Application Number" order by "Disbursal Entry Id") as disbursal_no,
        
        case when ai."Product Type Code" = 'PF' then 'Unsecured' else 'Secured' end as product_type,

			case when ai."Application Number" = 'APPL00000041' then 'Mr Krishna H'
			     when ai."Application Number" = 'APPL00000892' then 'Mr Arun Kumar'
			     when ai."Application Number" = 'APPL00000791' then 'Mr Aravind AR'
			     when ai."Application Number" = 'APPL00000314' then 'Mr Sai Kumar'
			     when ai."Application Number" = 'APPL00000314' then 'Mr Sai Kumar'
			     when ai."Application Number" = 'APPL00000859' then 'Mr Bhaleti Madhu'
			     when ai."Application Number" = 'APPL00001487' then 'Mr Siva SankaraRao'
			     when ai."Application Number" = 'APPL00001487' then 'Mr Siva SankaraRao'
			     when ai."Application Number" = 'APPL00000032' then 'Mr Shivaraj Kumar'
			     when ai."Application Number" = 'APPL00000848' then 'Mr M Anil Kumar'
			     when ai."Application Number" = 'APPL00000930' then 'Mr Bhaleti Madhu'
			     when ai."Application Number" = 'APPL00001303' then 'Mr Sammeta Ratnakar'
			     when ai."Application Number" = 'APPL00003922' then 'Mr Thammanaveni Ramesh'
			     when ai."Application Number" = 'APPL00005094' then 'Mr Thammanaveni Ramesh'
			else ai."Sourcing RM Name" end as sourcing_rm_name,
			
			case when ai."Application Number" = 'APPL00000041' then 'sf0083'
			     when ai."Application Number" = 'APPL00000892' then 'sf0082'
			     when ai."Application Number" = 'APPL00000791' then 'sf0137'
			     when ai."Application Number" = 'APPL00000314' then 'sf0046'
			     when ai."Application Number" = 'APPL00000314' then 'sf0046'
			     when ai."Application Number" = 'APPL00000859' then 'sf0233'
			     when ai."Application Number" = 'APPL00001487' then 'sf0106'
			     when ai."Application Number" = 'APPL00001487' then 'sf0106'
			     when ai."Application Number" = 'APPL00000032' then 'sf0080'
			     when ai."Application Number" = 'APPL00000848' then 'sf0257'
			     when ai."Application Number" = 'APPL00000930' then 'sf0233'
			     when ai."Application Number" = 'APPL00001303' then 'sf0246'
			     when ai."Application Number" = 'APPL00003922' then 'sf0187'
			     when ai."Application Number" = 'APPL00005094' then 'sf0187'
			else ai."Sourcing RM Code" end as sourcing_rm_code,
        CASE when ai."Application Number" in ('APPL00002567', 'APPL00003323', 'APPL00003972') then 'TS06' else ai."Branch Code" end as branch_code,
        CASE when ai."Application Number" in ('APPL00002567', 'APPL00003323', 'APPL00003972') then 'Mahbubnagar' else ai."Branch Name" end as branch_name,
        ai."Existing Customer" as existing_customer,
        ai."Application Received Date" as application_received_date_ts,
        case when ai."Loan Application Type" like 'Balance Transfer%' then 'BT' else 'Non-BT' end as loan_purpose,
        ai.product,
        substring(ai."Branch Code", 1, 2) as state_name,
        ai."Sanction Tenure" as sanctioned_tenure,
        ai."Sanctioned ROI" as sanctioned_roi,
        round(ai."Sanction Loan Amount", 0) as sanction_loan_amount,
        ai."Sanction Date" as sanctioned_date_ts,
		dd."Disbursal Status" as disbursal_status,
		ld."Loan Status" as loan_status
    FROM sf_neo_cas_lms."Disbursal Details" dd
    LEFT JOIN sf_neo_cas_lms."Application Information" ai 
        ON ai."Application Number" = dd."Application Number" 
	LEFT JOIN sf_neo_cas_lms."Loan Details" ld 
        ON ai."Application Number" = ld."File No"
    WHERE dd."Disbursal Status" = 'Disbursal Approved'
        AND dd."Disbursal Operation Id" IS NOT NULL
),

loan_aggregates AS (
    SELECT 
        disbursal_base.*,
        case when disbursal_base.disbursal_no = 1 then 
            (greatest(0, round(extract(epoch from (disbursal_base.disbursal_date_ts - disbursal_base.application_received_date_ts))/86400.0, 2))) 
            else null end as l2d_tat,
        case when disbursal_base.disbursal_no = 1 then 1 else 0 end as total_loans,
		case when disbursal_base.disbursal_no = 1 and disbursal_base.product_type = 'Secured' then 1 else 0 end as secured_files,
        disbursal_base.disbursal_amount as milestone_business
    FROM disbursal_base
),

-- Monthly aggregations for each RM with TAT calculation
monthly_data AS (
    SELECT 
        state_name,
        branch_name,
        branch_code,
        sourcing_rm_name,
        sourcing_rm_code,
        disb_month,
        disb_month_num,
        disb_year,
        disb_financial_year,
        disb_quarter,
        SUM(disbursal_amount) as monthly_disbursal_amount, 
        SUM(total_loans) as monthly_disbursals, 
        ROUND(AVG(CASE WHEN total_loans = 1 THEN l2d_tat END), 2) as avg_l2d_tat ,
		        -- Milestone metrics
        SUM(secured_files) as monthly_secured_files,
        SUM(milestone_business) as monthly_total_business
    FROM loan_aggregates
    GROUP BY state_name, branch_name, branch_code, sourcing_rm_name, sourcing_rm_code, disb_month, disb_month_num, disb_year, disb_financial_year, disb_quarter
),

-- Quarterly aggregations
quarterly_data AS (
    SELECT 
        state_name,
        branch_name,
        branch_code,
        sourcing_rm_name,
        sourcing_rm_code,
        disb_financial_year,
        disb_quarter,
        SUM(monthly_disbursal_amount) as quarterly_disbursal_amount,
        SUM(monthly_disbursals) as quarterly_disbursals
    FROM monthly_data
    GROUP BY state_name, branch_name, branch_code, sourcing_rm_name, sourcing_rm_code, disb_financial_year, disb_quarter
    HAVING SUM(monthly_disbursal_amount) > 0
),

-- Financial yearly aggregations
yearly_data AS (
    SELECT 
        state_name,
        branch_name,
        branch_code,
        sourcing_rm_name,
        sourcing_rm_code,
        disb_financial_year,
        SUM(monthly_disbursal_amount) as yearly_disbursal_amount,
        SUM(monthly_disbursals) as yearly_disbursals
    FROM monthly_data
    GROUP BY state_name, branch_name, branch_code, sourcing_rm_name, sourcing_rm_code, disb_financial_year
    HAVING SUM(monthly_disbursal_amount) > 0
),

-- Current Month Data
current_month_data AS (
    SELECT 
        dc.current_month_name,
        dc.financial_year,
        md.*
    FROM monthly_data md
    CROSS JOIN date_calculations dc
    WHERE md.disb_month = dc.current_month_name
        AND md.disb_year = dc.current_year
),

-- Previous Month Data
previous_month_data AS (
    SELECT 
        dc.previous_month_name,
        dc.financial_year,
        md.*
    FROM monthly_data md
    CROSS JOIN date_calculations dc
    WHERE md.disb_month = dc.previous_month_name
        AND md.disb_year = dc.previous_month_year
),

-- Current Quarter Data
current_quarter_data AS (
    SELECT 
        dc.current_quarter,
        dc.financial_year,
        qd.*
    FROM quarterly_data qd
    CROSS JOIN date_calculations dc
    WHERE qd.disb_quarter = dc.current_quarter
        AND qd.disb_financial_year = dc.financial_year
),

-- Current Financial Year Data
current_fy_data AS (
    SELECT 
        dc.financial_year,
        yd.*
    FROM yearly_data yd
    CROSS JOIN date_calculations dc
    WHERE yd.disb_financial_year = dc.financial_year
),

-- Enhanced disbursal data for month-wise aggregation
disbursal_data AS (
    SELECT 
        state_name,
        branch_name,
        branch_code,
        sourcing_rm_name,
        sourcing_rm_code,
        disb_month,
        monthly_disbursal_amount,
        monthly_disbursals,
        avg_l2d_tat,
		monthly_secured_files,
        monthly_total_business / 100000.0 as monthly_total_business_lakhs
    FROM monthly_data
),

-- IMD Payment Data Processing
mcas_paid AS (
    SELECT 
        application_number, 
        create_timestamp AS imd_paid_ts, 
        amount_to_pay
    FROM sf_neo_cas_lms."Cashfree IMD Mcas View SAARATHI"
    WHERE submit_date IS NOT NULL
        AND payment_status = 'PAID'
        AND create_timestamp >= DATE '2025-08-01'
        AND (amount_to_pay BETWEEN (0.95 * 1180) AND (1.05 * 1180) OR amount_to_pay = 999)
),

cas_paid AS (
    SELECT 
        application_number, 
        creation_time_stamp AS imd_paid_ts, 
        amount_to_pay
    FROM sf_neo_cas_lms."Cashfree IMD CAS View SAARATHI"
    WHERE payment_status = 'Paid'
        AND creation_time_stamp >= DATE '2025-08-01'
        AND (amount_to_pay BETWEEN (0.95 * 1180) AND (1.05 * 1180) OR amount_to_pay = 999)
),

paid_in_pg as (
	with base as(
		select distinct
		*
		from (
			SELECT
			pg_process_id,
			elem->>'payment_status' AS payment_status,
			elem->>'payment_amount' AS payment_amount,
			customer_phone,
			creation_time_stamp::date
			FROM sf_neo_int.pay_gateway_rqst_log_fis_info
			CROSS JOIN LATERAL jsonb_array_elements(response_body::jsonb) AS elem
			WHERE response_body LIKE '[%'
			AND elem->>'payment_status' = 'SUCCESS'
			AND creation_time_stamp >= DATE '2025-08-01'
		) a
		WHERE (a.payment_amount::NUMERIC BETWEEN (0.95 * 1180) AND (1.05 * 1180)
				 OR a.payment_amount::NUMERIC = 999)
		order by 4
	),
	
	all_cust as (
		select
		ai."Application Number" as app_no
		,'Main Applicant' as role
		,ai."Customer Number" as cust_no
		,ai."Neo CIF ID" as neo_cif_id
		,ai."Customer Name" as cust_name
		,'Main Applicant' as relation
		,cust."Mobile No"
		,case
			when cust."Primary Mobile" = 'Yes' then 'Primary'
			else 'Secondary'
		end as phone_number_type
		from sf_neo_cas_lms."Application Information" ai
		left join sf_neo_cas_lms."Communication Details" cust 
			on cust."Customer Number" = ai."Customer Number"
			
			
		union all
		
		select
		cb."Application Number" as app_no
		,'Co-Borrower' as role
		,cb."Co-Borrower Customer Number" as cust_no
		,cb."Neo CIF" as neo_cif_id
		,cb."Co-Borrower Name" as cust_name
		,cb."Relationship with Applicant" as relation
		,cust."Mobile No"
		,case
			when cust."Primary Mobile" = 'Yes' then 'Primary'
			else 'Secondary'
		end as phone_number_type
		from sf_neo_cas_lms."Co-Borrowers" cb
		left join sf_neo_cas_lms."Communication Details" cust
			on cust."Customer Number" = cb."Co-Borrower Customer Number"
		
		union all
		
		select
		ga."Application Number" as app_no
		,'Guarantor' as role
		,ga."Guarantor Customer Number" as cust_no
		,ga."Neo CIF" as neo_cif_id
		,ga."Guarantor Name" as cust_name
		,ga."Relationship with Applicant" as relation
		,cust."Mobile No"
		,case
			when cust."Primary Mobile" = 'Yes' then 'Primary'
			else 'Secondary'
		end as phone_number_type
		from sf_neo_cas_lms."Guarantor" ga
		left join sf_neo_cas_lms."Communication Details" cust
			on cust."Customer Number" = ga."Guarantor Customer Number"
	),
	
	final_base as (
		select
		a.*
		,ai."Application Received Date"
		from (
			select
			b.pg_process_id
			,b.payment_status
			,b.payment_amount
			,b.creation_time_stamp
			,b.customer_phone
			,ac.phone_number_type
			,ac.app_no
			,ac.role
			,ac.cust_no
			,ac.neo_cif_id
			,ac.cust_name
			,ac.relation
			,row_number() over(
				partition by 
					app_no,
					pg_process_id
				order by
					case
						when role = 'Main Applicant' then 1
						when role = 'Co-Borrower' then 2
						when role = 'Guarantor' then 3
						else 4
					end
			) as rn
			from base b
			left join all_cust ac on ac."Mobile No" = b.customer_phone
			where ac.neo_cif_id is not null
		) a
		left join sf_neo_cas_lms."Application Information" ai
			on ai."Application Number" = a.app_no
		where a.rn = 1
		order by app_no, pg_process_id
	),
	
	data_dump as(
		select
		*
		,row_number() over(
			partition by 
				pg_process_id
			order by
				diff_in_days
		) as priority
		from (
			select distinct
			f.*
			,ABS(creation_time_stamp::date - "Application Received Date"::date) AS diff_in_days
			,count(customer_phone) over (
				partition by
					pg_process_id
			)
			from final_base f
			order by 16 desc,customer_phone
		)
	)
	
	select
	*
	from data_dump
	where priority = 1
),

-- Enhanced login data with time period bifurcations
login_data_base AS (
    SELECT
        ai."Application Number" as application_number,
		substring(ai."Branch Code",1,2) as state_name,
        CASE when ai."Application Number" in ('APPL00002567', 'APPL00003323', 'APPL00003972') then 'Mahbubnagar' else ai."Branch Name" end as branch_name,
        CASE when ai."Application Number" in ('APPL00002567', 'APPL00003323', 'APPL00003972') then 'TS06' else ai."Branch Code" end as branch_code,

			case when ai."Application Number" = 'APPL00000041' then 'Mr Krishna H'
			     when ai."Application Number" = 'APPL00000892' then 'Mr Arun Kumar'
			     when ai."Application Number" = 'APPL00000791' then 'Mr Aravind AR'
			     when ai."Application Number" = 'APPL00000314' then 'Mr Sai Kumar'
			     when ai."Application Number" = 'APPL00000314' then 'Mr Sai Kumar'
			     when ai."Application Number" = 'APPL00000859' then 'Mr Bhaleti Madhu'
			     when ai."Application Number" = 'APPL00001487' then 'Mr Siva SankaraRao'
			     when ai."Application Number" = 'APPL00001487' then 'Mr Siva SankaraRao'
			     when ai."Application Number" = 'APPL00000032' then 'Mr Shivaraj Kumar'
			     when ai."Application Number" = 'APPL00000848' then 'Mr M Anil Kumar'
			     when ai."Application Number" = 'APPL00000930' then 'Mr Bhaleti Madhu'
			     when ai."Application Number" = 'APPL00001303' then 'Mr Sammeta Ratnakar'
			     when ai."Application Number" = 'APPL00003922' then 'Mr Thammanaveni Ramesh'
			     when ai."Application Number" = 'APPL00005094' then 'Mr Thammanaveni Ramesh'
			else ai."Sourcing RM Name" end as sourcing_rm_name,
			
			case when ai."Application Number" = 'APPL00000041' then 'sf0083'
			     when ai."Application Number" = 'APPL00000892' then 'sf0082'
			     when ai."Application Number" = 'APPL00000791' then 'sf0137'
			     when ai."Application Number" = 'APPL00000314' then 'sf0046'
			     when ai."Application Number" = 'APPL00000314' then 'sf0046'
			     when ai."Application Number" = 'APPL00000859' then 'sf0233'
			     when ai."Application Number" = 'APPL00001487' then 'sf0106'
			     when ai."Application Number" = 'APPL00001487' then 'sf0106'
			     when ai."Application Number" = 'APPL00000032' then 'sf0080'
			     when ai."Application Number" = 'APPL00000848' then 'sf0257'
			     when ai."Application Number" = 'APPL00000930' then 'sf0233'
			     when ai."Application Number" = 'APPL00001303' then 'sf0246'
			     when ai."Application Number" = 'APPL00003922' then 'sf0187'
			     when ai."Application Number" = 'APPL00005094' then 'sf0187'
			else ai."Sourcing RM Code" end as sourcing_rm_code,

        to_char(date(ai."Application Received Date"), 'Mon') as app_month,
        EXTRACT(MONTH FROM ai."Application Received Date") as app_month_num,
        EXTRACT(YEAR FROM ai."Application Received Date") as app_year,
        -- Financial year for each application
        CASE 
            WHEN EXTRACT(MONTH FROM ai."Application Received Date") >= 4 
            THEN EXTRACT(YEAR FROM ai."Application Received Date")
            ELSE EXTRACT(YEAR FROM ai."Application Received Date") - 1 
        END as app_financial_year,
        -- Quarter for each application
        CASE 
            WHEN EXTRACT(MONTH FROM ai."Application Received Date") IN (4,5,6) THEN 'Q1'
            WHEN EXTRACT(MONTH FROM ai."Application Received Date") IN (7,8,9) THEN 'Q2'
            WHEN EXTRACT(MONTH FROM ai."Application Received Date") IN (10,11,12) THEN 'Q3'
            ELSE 'Q4'
        END as app_quarter,

			case
				when ai."Application Number" in (select distinct application_number from mcas_paid) then 1
				when ai."Application Number" in (select distinct application_number from cas_paid) then 1
				when ai."Application Number" in (select distinct application_number from disbursal_base) then 1
				when ai."Application Number" in (select distinct app_no from paid_in_pg) then 1
				when ai."Application Received Date" < date '2025-08-01' then 1
				else 0
			end as monthly_applications
		
    FROM sf_neo_cas_lms."Application Information" ai
    WHERE 1=1
        AND (ai."Scheme Code" not in ('CONNECTOR') 
             OR ai."scheme" not in ('Connector Onboarding') 
             OR ai."Product Code" not in ('CONNECTOR'))
        AND ai."Application Number" NOT IN ('APPL00000787', 'APPL00000282', 'APPL00000309', 'APPL00003680')
		AND (
		    -- For Apr, May, Jun, Jul - count all applications
		    (to_char(date(ai."Application Received Date"), 'Mon') IN ('Apr', 'May', 'Jun', 'Jul'))
		    OR 
		    -- For other months - only count if IMD received or disbursed
		    (to_char(date(ai."Application Received Date"), 'Mon') NOT IN ('Apr', 'May', 'Jun', 'Jul'))
		)
),

login_data AS (
    SELECT 
        state_name,
        branch_name,
        branch_code,
        sourcing_rm_name,
        sourcing_rm_code,
        app_month,
        app_month_num,
        app_year,
		app_financial_year,
		app_quarter,
        SUM(monthly_applications) as monthly_applications
    FROM login_data_base
    GROUP BY state_name, branch_name, branch_code, sourcing_rm_name, sourcing_rm_code, app_month, app_month_num, app_year, app_quarter, app_financial_year
),


-- Current Month Login Data
current_month_login AS (
    SELECT 
        dc.current_month_name,
        dc.financial_year,
        ld.*
    FROM login_data ld
    CROSS JOIN date_calculations dc
    WHERE ld.app_month = dc.current_month_name
        AND ld.app_year = dc.current_year
),

-- Previous Month Login Data
previous_month_login AS (
    SELECT 
        dc.previous_month_name,
        dc.financial_year,
        ld.*
    FROM login_data ld
    CROSS JOIN date_calculations dc
    WHERE ld.app_month = dc.previous_month_name
        AND ld.app_year = dc.previous_month_year
),

-- Current Quarter Login Data
current_quarter_login AS (
    SELECT 
        state_name,
        branch_name,
        branch_code,
        sourcing_rm_name,
        sourcing_rm_code,
        app_financial_year,
        app_quarter,
        SUM(monthly_applications) as quarterly_applications
    FROM login_data ld
    CROSS JOIN date_calculations dc
    WHERE ld.app_quarter = dc.current_quarter
        AND ld.app_financial_year = dc.financial_year
    GROUP BY state_name, branch_name, branch_code, sourcing_rm_name, sourcing_rm_code, app_financial_year, app_quarter
),

-- Current Financial Year Login Data
current_fy_login AS (
    SELECT 
        state_name,
        branch_name,
        branch_code,
        sourcing_rm_name,
        sourcing_rm_code,
        app_financial_year,
        SUM(monthly_applications) as yearly_applications
    FROM login_data ld
    CROSS JOIN date_calculations dc
    WHERE ld.app_financial_year = dc.financial_year
    GROUP BY state_name, branch_name, branch_code, sourcing_rm_name, sourcing_rm_code, app_financial_year
),

all_rm_months AS (
    SELECT DISTINCT 
        all_rms.state_name, all_rms.branch_name, all_rms.branch_code,
        all_rms.sourcing_rm_name, all_rms.sourcing_rm_code,
        all_months.month_name
    FROM (
        SELECT state_name, branch_name, branch_code, sourcing_rm_name, sourcing_rm_code FROM disbursal_data
        UNION
        SELECT state_name, branch_name, branch_code, sourcing_rm_name, sourcing_rm_code FROM login_data
    ) all_rms
    CROSS JOIN (
        SELECT DISTINCT month_name FROM (
            SELECT disb_month as month_name FROM disbursal_data
            UNION
            SELECT app_month as month_name FROM login_data
        ) months
    ) all_months
),

combined_data0 AS (
    SELECT 
        arm.state_name,
        arm.branch_name,
        arm.branch_code,
        arm.sourcing_rm_name,
        arm.sourcing_rm_code,
        arm.month_name,
        COALESCE(ld.monthly_applications, 0) as applications,
        COALESCE(dd.monthly_disbursals, 0) as disbursals,
        COALESCE(dd.monthly_disbursal_amount, 0) as disbursal_amount,
        dd.avg_l2d_tat as avg_l2d_tat,
		COALESCE(dd.monthly_secured_files, 0) as secured_files,
        COALESCE(dd.monthly_total_business_lakhs, 0) as total_business_lakhs
    FROM all_rm_months arm
    LEFT JOIN login_data ld ON (
        arm.state_name = ld.state_name 
        AND arm.branch_name = ld.branch_name 
        AND arm.branch_code = ld.branch_code
        AND arm.sourcing_rm_name = ld.sourcing_rm_name
        AND arm.sourcing_rm_code = ld.sourcing_rm_code
        AND arm.month_name = ld.app_month
    )
    LEFT JOIN disbursal_data dd ON (
        arm.state_name = dd.state_name 
        AND arm.branch_name = dd.branch_name 
        AND arm.branch_code = dd.branch_code
        AND arm.sourcing_rm_name = dd.sourcing_rm_name
        AND arm.sourcing_rm_code = dd.sourcing_rm_code
        AND arm.month_name = dd.disb_month
    )
),

-- Calculate milestones for each RM-month combination
combined_data AS (
    SELECT 
        *,
        -- Milestone logic: Check secured files + total business criteria
        CASE 
            WHEN secured_files >= 4 AND total_business_lakhs >= 30 THEN 'Platinum'
            WHEN secured_files >= 3 AND total_business_lakhs >= 20 THEN 'Gold'
            WHEN secured_files >= 2 AND total_business_lakhs >= 15 THEN 'Silver'
            ELSE '-'
        END as milestone_achieved
    FROM combined_data0
)
-- , rm_data_mom as (
SELECT 
    state_name,
    branch_name,
    branch_code,
    sourcing_rm_name,
    sourcing_rm_code,
    
    -- Applications by Month
    SUM(CASE WHEN month_name = 'Jan' THEN applications ELSE 0 END) as "Jan - App",
    SUM(CASE WHEN month_name = 'Feb' THEN applications ELSE 0 END) as "Feb - App",
    SUM(CASE WHEN month_name = 'Mar' THEN applications ELSE 0 END) as "Mar - App",
    SUM(CASE WHEN month_name = 'Apr' THEN applications ELSE 0 END) as "Apr - App",
    SUM(CASE WHEN month_name = 'May' THEN applications ELSE 0 END) as "May - App",
    SUM(CASE WHEN month_name = 'Jun' THEN applications ELSE 0 END) as "Jun - App",
    SUM(CASE WHEN month_name = 'Jul' THEN applications ELSE 0 END) as "Jul - App",
    SUM(CASE WHEN month_name = 'Aug' THEN applications ELSE 0 END) as "Aug - App",
    SUM(CASE WHEN month_name = 'Sep' THEN applications ELSE 0 END) as "Sep - App",
    SUM(CASE WHEN month_name = 'Oct' THEN applications ELSE 0 END) as "Oct - App",
    SUM(CASE WHEN month_name = 'Nov' THEN applications ELSE 0 END) as "Nov - App",
    SUM(CASE WHEN month_name = 'Dec' THEN applications ELSE 0 END) as "Dec - App",
    
    -- Disbursals by Month
    SUM(CASE WHEN month_name = 'Jan' THEN disbursals ELSE 0 END) as "Jan - Disb",
    SUM(CASE WHEN month_name = 'Feb' THEN disbursals ELSE 0 END) as "Feb - Disb",
    SUM(CASE WHEN month_name = 'Mar' THEN disbursals ELSE 0 END) as "Mar - Disb",
    SUM(CASE WHEN month_name = 'Apr' THEN disbursals ELSE 0 END) as "Apr - Disb",
    SUM(CASE WHEN month_name = 'May' THEN disbursals ELSE 0 END) as "May - Disb",
    SUM(CASE WHEN month_name = 'Jun' THEN disbursals ELSE 0 END) as "Jun - Disb",
    SUM(CASE WHEN month_name = 'Jul' THEN disbursals ELSE 0 END) as "Jul - Disb",
    SUM(CASE WHEN month_name = 'Aug' THEN disbursals ELSE 0 END) as "Aug - Disb",
    SUM(CASE WHEN month_name = 'Sep' THEN disbursals ELSE 0 END) as "Sep - Disb",
    SUM(CASE WHEN month_name = 'Oct' THEN disbursals ELSE 0 END) as "Oct - Disb",
    SUM(CASE WHEN month_name = 'Nov' THEN disbursals ELSE 0 END) as "Nov - Disb",
    SUM(CASE WHEN month_name = 'Dec' THEN disbursals ELSE 0 END) as "Dec - Disb",
    
    -- Disbursal Amount by Month
    SUM(CASE WHEN month_name = 'Jan' THEN disbursal_amount ELSE 0 END) as "Jan - Disb Amt",
    SUM(CASE WHEN month_name = 'Feb' THEN disbursal_amount ELSE 0 END) as "Feb - Disb Amt",
    SUM(CASE WHEN month_name = 'Mar' THEN disbursal_amount ELSE 0 END) as "Mar - Disb Amt",
    SUM(CASE WHEN month_name = 'Apr' THEN disbursal_amount ELSE 0 END) as "Apr - Disb Amt",
    SUM(CASE WHEN month_name = 'May' THEN disbursal_amount ELSE 0 END) as "May - Disb Amt",
    SUM(CASE WHEN month_name = 'Jun' THEN disbursal_amount ELSE 0 END) as "Jun - Disb Amt",
    SUM(CASE WHEN month_name = 'Jul' THEN disbursal_amount ELSE 0 END) as "Jul - Disb Amt",
    SUM(CASE WHEN month_name = 'Aug' THEN disbursal_amount ELSE 0 END) as "Aug - Disb Amt",
    SUM(CASE WHEN month_name = 'Sep' THEN disbursal_amount ELSE 0 END) as "Sep - Disb Amt",
    SUM(CASE WHEN month_name = 'Oct' THEN disbursal_amount ELSE 0 END) as "Oct - Disb Amt",
    SUM(CASE WHEN month_name = 'Nov' THEN disbursal_amount ELSE 0 END) as "Nov - Disb Amt",
    SUM(CASE WHEN month_name = 'Dec' THEN disbursal_amount ELSE 0 END) as "Dec - Disb Amt",
    
    -- Average L2D TAT by Month
    ROUND(AVG(CASE WHEN month_name = 'Jan' THEN avg_l2d_tat END), 2) as "Jan - TAT",
    ROUND(AVG(CASE WHEN month_name = 'Feb' THEN avg_l2d_tat END), 2) as "Feb - TAT",
    ROUND(AVG(CASE WHEN month_name = 'Mar' THEN avg_l2d_tat END), 2) as "Mar - TAT",
    ROUND(AVG(CASE WHEN month_name = 'Apr' THEN avg_l2d_tat END), 2) as "Apr - TAT",
    ROUND(AVG(CASE WHEN month_name = 'May' THEN avg_l2d_tat END), 2) as "May - TAT",
    ROUND(AVG(CASE WHEN month_name = 'Jun' THEN avg_l2d_tat END), 2) as "Jun - TAT",
    ROUND(AVG(CASE WHEN month_name = 'Jul' THEN avg_l2d_tat END), 2) as "Jul - TAT",
    ROUND(AVG(CASE WHEN month_name = 'Aug' THEN avg_l2d_tat END), 2) as "Aug - TAT",
    ROUND(AVG(CASE WHEN month_name = 'Sep' THEN avg_l2d_tat END), 2) as "Sep - TAT",
    ROUND(AVG(CASE WHEN month_name = 'Oct' THEN avg_l2d_tat END), 2) as "Oct - TAT",
    ROUND(AVG(CASE WHEN month_name = 'Nov' THEN avg_l2d_tat END), 2) as "Nov - TAT",
    ROUND(AVG(CASE WHEN month_name = 'Dec' THEN avg_l2d_tat END), 2) as "Dec - TAT",

	-- MILESTONE ACHIEVEMENTS by Month
    coalesce(MAX(CASE WHEN month_name = 'Jan' THEN milestone_achieved END),'-') as "Jan - Milestone",
    coalesce(MAX(CASE WHEN month_name = 'Feb' THEN milestone_achieved END),'-') as "Feb - Milestone",
    coalesce(MAX(CASE WHEN month_name = 'Mar' THEN milestone_achieved END),'-') as "Mar - Milestone",
    coalesce(MAX(CASE WHEN month_name = 'Apr' THEN milestone_achieved END),'-') as "Apr - Milestone",
    coalesce(MAX(CASE WHEN month_name = 'May' THEN milestone_achieved END),'-') as "May - Milestone",
    coalesce(MAX(CASE WHEN month_name = 'Jun' THEN milestone_achieved END),'-') as "Jun - Milestone",
    coalesce(MAX(CASE WHEN month_name = 'Jul' THEN milestone_achieved END),'-') as "Jul - Milestone",
    coalesce(MAX(CASE WHEN month_name = 'Aug' THEN milestone_achieved END),'-') as "Aug - Milestone",
    coalesce(MAX(CASE WHEN month_name = 'Sep' THEN milestone_achieved END),'-') as "Sep - Milestone",
    coalesce(MAX(CASE WHEN month_name = 'Oct' THEN milestone_achieved END),'-') as "Oct - Milestone",
    coalesce(MAX(CASE WHEN month_name = 'Nov' THEN milestone_achieved END),'-') as "Nov - Milestone",
    coalesce(MAX(CASE WHEN month_name = 'Dec' THEN milestone_achieved END),'-') as "Dec - Milestone",
	
    -- Total Summary Columns
    SUM(applications) as "Total Applications",
    SUM(disbursals) as "Total Disbursals",
    SUM(disbursal_amount) as "Total Disbursal Amount"
    ,ROUND(AVG(avg_l2d_tat), 2) as "Overall Average TAT",

	-- Current Financial Quarter Totals (Q1: Apr-Jun, Q2: Jul-Sep, Q3: Oct-Dec, Q4: Jan-Mar)
    SUM(CASE 
        WHEN (EXTRACT(MONTH FROM CURRENT_DATE) IN (4,5,6) AND month_name IN ('Apr','May','Jun'))
          OR (EXTRACT(MONTH FROM CURRENT_DATE) IN (7,8,9) AND month_name IN ('Jul','Aug','Sep'))  
          OR (EXTRACT(MONTH FROM CURRENT_DATE) IN (10,11,12) AND month_name IN ('Oct','Nov','Dec'))
          OR (EXTRACT(MONTH FROM CURRENT_DATE) IN (1,2,3) AND month_name IN ('Jan','Feb','Mar'))
        THEN applications ELSE 0 
    END) as "Current Quarter Applications",
    
    SUM(CASE 
        WHEN (EXTRACT(MONTH FROM CURRENT_DATE) IN (4,5,6) AND month_name IN ('Apr','May','Jun'))
          OR (EXTRACT(MONTH FROM CURRENT_DATE) IN (7,8,9) AND month_name IN ('Jul','Aug','Sep'))
          OR (EXTRACT(MONTH FROM CURRENT_DATE) IN (10,11,12) AND month_name IN ('Oct','Nov','Dec'))
          OR (EXTRACT(MONTH FROM CURRENT_DATE) IN (1,2,3) AND month_name IN ('Jan','Feb','Mar'))
        THEN disbursals ELSE 0 
    END) as "Current Quarter Disbursals",
    
    SUM(CASE 
        WHEN (EXTRACT(MONTH FROM CURRENT_DATE) IN (4,5,6) AND month_name IN ('Apr','May','Jun'))
          OR (EXTRACT(MONTH FROM CURRENT_DATE) IN (7,8,9) AND month_name IN ('Jul','Aug','Sep'))
          OR (EXTRACT(MONTH FROM CURRENT_DATE) IN (10,11,12) AND month_name IN ('Oct','Nov','Dec'))
          OR (EXTRACT(MONTH FROM CURRENT_DATE) IN (1,2,3) AND month_name IN ('Jan','Feb','Mar'))
        THEN disbursal_amount ELSE 0 
    END) as "Current Quarter Disbursal Amount",
    
    -- Current Financial Year Totals (Apr-Mar cycle)
    SUM(CASE 
        WHEN (EXTRACT(MONTH FROM CURRENT_DATE) >= 4 AND month_name IN ('Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'))
          OR (EXTRACT(MONTH FROM CURRENT_DATE) < 4 AND month_name IN ('Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec','Jan','Feb','Mar'))
        THEN applications ELSE 0 
    END) as "Current FY Applications",
    
    SUM(CASE 
        WHEN (EXTRACT(MONTH FROM CURRENT_DATE) >= 4 AND month_name IN ('Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'))
          OR (EXTRACT(MONTH FROM CURRENT_DATE) < 4 AND month_name IN ('Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec','Jan','Feb','Mar'))
        THEN disbursals ELSE 0 
    END) as "Current FY Disbursals",
    
    SUM(CASE 
        WHEN (EXTRACT(MONTH FROM CURRENT_DATE) >= 4 AND month_name IN ('Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'))
          OR (EXTRACT(MONTH FROM CURRENT_DATE) < 4 AND month_name IN ('Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec','Jan','Feb','Mar'))
        THEN disbursal_amount ELSE 0 
    END) as "Current FY Disbursal Amount"

FROM combined_data
GROUP BY state_name, branch_name, branch_code, sourcing_rm_name, sourcing_rm_code
ORDER BY state_name, branch_name, sourcing_rm_name

-- ),
