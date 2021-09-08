setDBdata_query = """
SELECT pjd.work_user,
        pjd.check_user,
       mem.member_nm AS WORK_nm,
       mem2.member_nm AS CHECK_nm,
       pjd.data_idx,
       pjd.prog_state_cd,
       pjd.src_idx as source_id,
       pjd.work_edate AS work_enddate,
       pjd.check_edate AS check_enddate,
       mem.gender_cd,
       CASE WHEN LEFT(mem.birthday, 4) = "0000"
           THEN 0
           WHEN mem.birthday is null THEN 0
           ELSE YEAR(NOW()) - LEFT(mem.birthday, 4)
           END age,
       mem.residence1,
       mem.login_id,
       mem.mobile_no
FROM CW_PROJECT pj
         INNER JOIN TB_PRJ_MST pjm
                 ON (pj.project_id = pjm.project_id
                AND pj.project_id in ({proId}))
         INNER JOIN TB_PRJ_DATA pjd
                 ON (pjm.prj_idx = pjd.prj_idx)
         INNER JOIN TB_MEMBER mem
                 ON (pjd.work_user = mem.member_id)
        LEFT JOIN TB_MEMBER mem2
                 ON (pjd.check_user = mem.member_id)
where pjd.prog_state_cd IN ({whereInList}) {queryFilter} {orderBy}

LIMIT {limit} OFFSET {offset_str}

;

"""

# 기존에 닉네임, 핸드폰 번호 추가
setDBdata_nick_phone_query= """
SELECT pjd.data_idx,
pj.project_id,
pjd.src_idx as source_id,
check_edate,
work_edate,
mem.login_id,
mem.member_id,
project_name,
mem.gender_cd


FROM CW_PROJECT pj
         INNER JOIN TB_PRJ_MST pjm
                 ON (pj.project_id = pjm.project_id
                AND pj.project_id in ({proId}))
         INNER JOIN TB_PRJ_DATA pjd
                 ON (pjm.prj_idx = pjd.prj_idx)
         INNER JOIN TB_MEMBER mem
                    ON (pjd.work_user = mem.member_id)
where
{data_idx}
{whereInList}
{queryFilter} 
{orderBy}
LIMIT {limit} OFFSET {offset_str};

"""
# setDBdata_nick_phone_query = '''
# SELECT prj2.src_idx,
#         prj2.data_idx,
# 		prj2.work_edate,
# 		ext_id,
# 		prj2.work_user
# FROM TB_PRJ_DATA prj2
# Right JOIN(
# SELECT 
# 		data_idx,
# 		prog_state_cd,
#         member_id,
#         ext_id,
#         src_idx,
#         MAX(work_edate) AS max_items

# FROM CW_PROJECT pj
#          INNER JOIN TB_PRJ_MST pjm
#                  ON (pj.project_id = pjm.project_id
#                 AND pj.project_id in (8783))
#          INNER JOIN TB_PRJ_DATA pjd
#                  ON (pjm.prj_idx = pjd.prj_idx)
#          INNER JOIN member_ext_info mem
#                     ON (pjd.work_user = mem.member_id)

# AND pjd.prog_state_cd IN ("WORK_END")  AND pjd.work_edate BETWEEN "2021-07-23 00:00:00" AND "2021-08-02 23:59:59" 
# GROUP BY ext_id) tt1
# ON prj2.work_user = tt1.member_id 
# AND prj2.work_edate = tt1.max_items
# '''

dataSet_query = """
SELECT
       pjd.work_user,
       pjd.data_idx,
       mem.member_id,
       pjd.result_json
FROM CW_PROJECT pj
         INNER JOIN TB_PRJ_MST pjm
                    ON (pj.project_id = pjm.project_id
                        AND pj.project_id in ({proId}))
         INNER JOIN TB_PRJ_DATA pjd
                    ON (pjm.prj_idx = pjd.prj_idx)
         INNER JOIN TB_MEMBER mem
                    ON (pjd.work_user = mem.member_id)
"""







# JIRA 2514
setDBdata_query_2515 = """
SELECT pjd.work_user,
       mem.member_nm,
       pjd.data_idx,
       pjd.prog_state_cd,
       pjd.src_idx as source_id,
       pjd.work_sdate,
       pjd.work_edate,
       mem.gender_cd,
       CASE WHEN LEFT(mem.birthday, 4) = "0000"
           THEN 0
           WHEN mem.birthday is null THEN 0
           ELSE YEAR(NOW()) - LEFT(mem.birthday, 4)
           END age,
       residence1,
       mem.login_id,
       mem.mobile_no,
       mem.nickname
FROM CW_PROJECT pj
         INNER JOIN TB_PRJ_MST pjm
                 ON (pj.project_id = pjm.project_id
                AND pj.project_id in ({proId}))
         INNER JOIN TB_PRJ_DATA pjd
                 ON (pjm.prj_idx = pjd.prj_idx)
         INNER JOIN TB_MEMBER mem
                 ON (pjd.work_user = mem.member_id)
where pjd.prog_state_cd IN ({whereInList}) {queryFilter} {orderBy}
LIMIT {limit} OFFSET {offset_str};
"""