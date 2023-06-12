# Databricks notebook source
# MAGIC %md # CCU051_D08_sample_weights
# MAGIC 
# MAGIC **Description** This notebook creates the weights for CCU051.
# MAGIC  * Based on Census 2021 estimates by sex and age
# MAGIC  * Based on Census 2021 estimates by sex, age and region
# MAGIC  * Based on health care attendance in the last 5 years
# MAGIC   
# MAGIC **Author(s)** Genevieve Cezard, Tom Bolton
# MAGIC 
# MAGIC **Project** CCU051
# MAGIC 
# MAGIC **First copied over** 2023.01.16 (based on test notebook created by Tom Bolton from CCU051/tmp)
# MAGIC 
# MAGIC **Date last updated** 2023.01.27
# MAGIC 
# MAGIC **Date last run** 2023.01.27
# MAGIC 
# MAGIC **Data input** functions - libraries - parameters - manual population estimates input\
# MAGIC 'ccu051_out_cohort'\
# MAGIC deaths - gdppr - hes_apc - hes_op - hes_ae - vacc - sgss - pmeds 
# MAGIC 
# MAGIC **Data output** - 'CCU051_out_weights'

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

# DBTITLE 1,Libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import databricks.koalas as ks
import pandas as pd
import numpy as np

import re
import io
import datetime

import matplotlib
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import seaborn as sns

print("Matplotlib version: ", matplotlib.__version__)
print("Seaborn version: ", sns.__version__)
_datetimenow = datetime.datetime.now() # .strftime("%Y%m%d")
print(f"_datetimenow:  {_datetimenow}")

# COMMAND ----------

# DBTITLE 1,Functions
# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/SHDS/common/functions"

# COMMAND ----------

# MAGIC %md # 0. Parameters

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU051/CCU051_D01_parameters"

# COMMAND ----------

# MAGIC %md # 1. Data

# COMMAND ----------

spark.sql(f"""REFRESH TABLE {dbc}.{proj}_out_cohort""")
cohort = spark.table(f'{dbc}.{proj}_out_cohort')

# COMMAND ----------

display(cohort)

# COMMAND ----------

# select variables and add region
_cohort = cohort\
  .select('PERSON_ID', 'AGE', 'SEX', 'region')\
  .where(f.col('PERSON_ID').isNotNull())


# COMMAND ----------

# MAGIC %md # 2. Get population estimates for England

# COMMAND ----------

# MAGIC %md ## 2.1. Mid-2020 population estimates by sex and age

# COMMAND ----------

# Mid-2020 population estimates
# see BHFDSC_Analysis\20221222_
pop = """
SEX,AGE,pe_mid2020
1,0,308815
1,1,321378
1,2,333560
1,3,344089
1,4,354452
1,5,353655
1,6,356004
1,7,363986
1,8,374129
1,9,366587
1,10,360144
1,11,355213
1,12,358555
1,13,347682
1,14,340280
1,15,327124
1,16,322370
1,17,315497
1,18,312761
1,19,323700
1,20,337659
1,21,350920
1,22,358420
1,23,370801
1,24,373901
1,25,371883
1,26,382502
1,27,381754
1,28,388663
1,29,399614
1,30,392851
1,31,384813
1,32,385952
1,33,372700
1,34,380096
1,35,378573
1,36,367247
1,37,369196
1,38,367387
1,39,370566
1,40,371661
1,41,358524
1,42,335609
1,43,329766
1,44,334708
1,45,340715
1,46,345236
1,47,359601
1,48,374360
1,49,383296
1,50,374154
1,51,382152
1,52,381638
1,53,388278
1,54,385096
1,55,386043
1,56,381939
1,57,372235
1,58,363185
1,59,349191
1,60,333011
1,61,324227
1,62,316683
1,63,303232
1,64,291336
1,65,278378
1,66,276173
1,67,271510
1,68,260801
1,69,260852
1,70,263006
1,71,268821
1,72,280766
1,73,302439
1,74,228895
1,75,217830
1,76,213013
1,77,192013
1,78,166567
1,79,144651
1,80,146189
1,81,139908
1,82,130442
1,83,118033
1,84,105446
1,85,93860
1,86,80158
1,87,69745
1,88,60874
1,89,51545
1,90,169548
2,0,293098
2,1,304098
2,2,316666
2,3,326927
2,4,336364
2,5,335535
2,6,338730
2,7,345954
2,8,356419
2,9,348459
2,10,342943
2,11,337660
2,12,340266
2,13,329091
2,14,323745
2,15,310632
2,16,305653
2,17,297528
2,18,293850
2,19,306756
2,20,315496
2,21,328567
2,22,335796
2,23,349599
2,24,351363
2,25,353228
2,26,363407
2,27,365436
2,28,379850
2,29,385156
2,30,379113
2,31,379925
2,32,387224
2,33,381253
2,34,380725
2,35,380382
2,36,373787
2,37,376713
2,38,376427
2,39,377931
2,40,378222
2,41,361624
2,42,336647
2,43,331442
2,44,338100
2,45,344586
2,46,350995
2,47,365074
2,48,380075
2,49,394701
2,50,384751
2,51,393563
2,52,392445
2,53,395838
2,54,397436
2,55,398256
2,56,393381
2,57,383788
2,58,373754
2,59,360010
2,60,344951
2,61,336192
2,62,328213
2,63,315661
2,64,303307
2,65,292765
2,66,293735
2,67,288334
2,68,280496
2,69,281256
2,70,287053
2,71,291941
2,72,307142
2,73,330413
2,74,253652
2,75,244025
2,76,240429
2,77,221693
2,78,195845
2,79,173926
2,80,177589
2,81,173173
2,82,164396
2,83,153030
2,84,140983
2,85,129198
2,86,114852
2,87,104116
2,88,95623
2,89,85372
2,90,351519
"""
pop = spark.createDataFrame(pd.DataFrame(pd.read_csv(io.StringIO(pop))))


# COMMAND ----------

display(pop)

# COMMAND ----------

tmph = (
  pop
  .groupBy()
  .agg(f.sum(f.col('pe_mid2020')).alias('pe_mid2020_sum'))
)
display(tmph)

# COMMAND ----------

# MAGIC %md ## 2.2. Census-2021 population estimates by sex and age

# COMMAND ----------

# Census 2021 population estimates for England by sex and age
pop_census2021 = """
SEX,AGE,pe_census2021
1,0,282933
1,1,295634
1,2,301599
1,3,306032
1,4,315442
1,5,321107
1,6,317726
1,7,321907
1,8,334272
1,9,339456
1,10,337922
1,11,335465
1,12,334871
1,13,332212
1,14,323711
1,15,313590
1,16,315209
1,17,311182
1,18,308256
1,19,319917
1,20,319929
1,21,330231
1,22,342462
1,23,350317
1,24,358627
1,25,359798
1,26,370055
1,27,379832
1,28,388230
1,29,399459
1,30,410249
1,31,407064
1,32,409408
1,33,413828
1,34,403811
1,35,400036
1,36,395941
1,37,388142
1,38,385261
1,39,384105
1,40,393488
1,41,385950
1,42,363845
1,43,341802
1,44,341634
1,45,346214
1,46,354480
1,47,359914
1,48,374715
1,49,390222
1,50,399771
1,51,390018
1,52,399572
1,53,397200
1,54,398328
1,55,399263
1,56,398741
1,57,389462
1,58,380825
1,59,368413
1,60,354854
1,61,337895
1,62,332120
1,63,321213
1,64,307955
1,65,294275
1,66,288358
1,67,288286
1,68,279734
1,69,274921
1,70,275551
1,71,282627
1,72,286708
1,73,311493
1,74,308504
1,75,233621
1,76,245200
1,77,224582
1,78,209044
1,79,179080
1,80,168748
1,81,171871
1,82,163822
1,83,152911
1,84,141192
1,85,128329
1,86,115296
1,87,102191
1,88,92689
1,89,82402
1,90,339160
2,0,296055
2,1,310301
2,2,317434
2,3,320330
2,4,331189
2,5,337582
2,6,332834
2,7,336955
2,8,351858
2,9,355003
2,10,356597
2,11,352255
2,12,350387
2,13,350411
2,14,339496
2,15,330524
2,16,334252
2,17,330865
2,18,324799
2,19,330301
2,20,329370
2,21,338097
2,22,345066
2,23,347977
2,24,352375
2,25,352107
2,26,357353
2,27,363058
2,28,368108
2,29,377283
2,30,387679
2,31,381724
2,32,381229
2,33,385229
2,34,372364
2,35,376410
2,36,371338
2,37,365390
2,38,363598
2,39,365169
2,40,372710
2,41,368248
2,42,350507
2,43,330754
2,44,331468
2,45,336594
2,46,345229
2,47,350581
2,48,365326
2,49,379370
2,50,385521
2,51,378728
2,52,386717
2,53,384198
2,54,387701
2,55,384855
2,56,385216
2,57,375674
2,58,367810
2,59,356110
2,60,346541
2,61,328541
2,62,320308
2,63,311104
2,64,295467
2,65,281753
2,66,272253
2,67,271693
2,68,260946
2,69,255277
2,70,254130
2,71,256837
2,72,261227
2,73,281632
2,74,277941
2,75,207744
2,76,216053
2,77,195462
2,78,178766
2,79,149225
2,80,137721
2,81,137720
2,82,128775
2,83,117553
2,84,105770
2,85,92262
2,86,80786
2,87,68638
2,88,59523
2,89,50372
2,90,160653
"""
pop_census2021 = spark.createDataFrame(pd.DataFrame(pd.read_csv(io.StringIO(pop_census2021))))

# COMMAND ----------

display(pop_census2021)

# COMMAND ----------

tmpg = (
  pop_census2021
  .groupBy()
  .agg(f.sum(f.col('pe_census2021')).alias('pe_census2021_sum'))
)
display(tmpg)

# COMMAND ----------

# MAGIC %md ## 2.3. Census-2021 population estimates by sex,age and region

# COMMAND ----------

# Census 2021 population estimates for England by sex and age
pop_census2021_region = """
SEX,AGE,region,pe_census2021
1,0,North East,11975
1,1,North East,12750
1,2,North East,13084
1,3,North East,13516
1,4,North East,14206
1,5,North East,14199
1,6,North East,14105
1,7,North East,14391
1,8,North East,14996
1,9,North East,15311
1,10,North East,15301
1,11,North East,15145
1,12,North East,15024
1,13,North East,15094
1,14,North East,14708
1,15,North East,14118
1,16,North East,14203
1,17,North East,13898
1,18,North East,14750
1,19,North East,16607
1,20,North East,16495
1,21,North East,16319
1,22,North East,15986
1,23,North East,15635
1,24,North East,16110
1,25,North East,15515
1,26,North East,15979
1,27,North East,16355
1,28,North East,16554
1,29,North East,17438
1,30,North East,17474
1,31,North East,16915
1,32,North East,17245
1,33,North East,17840
1,34,North East,17567
1,35,North East,17292
1,36,North East,17213
1,37,North East,16491
1,38,North East,16533
1,39,North East,16509
1,40,North East,17024
1,41,North East,16821
1,42,North East,15631
1,43,North East,14106
1,44,North East,14260
1,45,North East,15092
1,46,North East,15191
1,47,North East,15849
1,48,North East,16757
1,49,North East,18347
1,50,North East,18655
1,51,North East,18092
1,52,North East,18760
1,53,North East,18845
1,54,North East,19445
1,55,North East,19642
1,56,North East,19782
1,57,North East,19934
1,58,North East,19539
1,59,North East,19391
1,60,North East,18530
1,61,North East,18072
1,62,North East,18321
1,63,North East,17791
1,64,North East,16885
1,65,North East,16279
1,66,North East,15590
1,67,North East,15586
1,68,North East,15265
1,69,North East,14757
1,70,North East,14799
1,71,North East,15091
1,72,North East,15130
1,73,North East,16290
1,74,North East,15975
1,75,North East,11823
1,76,North East,12381
1,77,North East,10797
1,78,North East,10013
1,79,North East,8934
1,80,North East,8841
1,81,North East,8736
1,82,North East,8538
1,83,North East,8145
1,84,North East,7360
1,85,North East,6598
1,86,North East,5891
1,87,North East,5122
1,88,North East,4647
1,89,North East,4063
1,90,North East,15359
2,0,North East,12799
2,1,North East,13092
2,2,North East,13859
2,3,North East,14220
2,4,North East,14804
2,5,North East,15236
2,6,North East,14791
2,7,North East,15428
2,8,North East,16004
2,9,North East,16012
2,10,North East,16406
2,11,North East,15998
2,12,North East,15641
2,13,North East,15711
2,14,North East,15363
2,15,North East,14767
2,16,North East,14737
2,17,North East,14413
2,18,North East,15378
2,19,North East,17238
2,20,North East,16932
2,21,North East,17162
2,22,North East,16511
2,23,North East,16011
2,24,North East,15765
2,25,North East,15544
2,26,North East,15476
2,27,North East,15639
2,28,North East,16017
2,29,North East,16215
2,30,North East,16536
2,31,North East,15933
2,32,North East,15988
2,33,North East,16356
2,34,North East,16121
2,35,North East,16599
2,36,North East,15885
2,37,North East,15773
2,38,North East,15603
2,39,North East,15502
2,40,North East,15815
2,41,North East,16029
2,42,North East,15260
2,43,North East,13834
2,44,North East,13387
2,45,North East,14336
2,46,North East,14722
2,47,North East,15075
2,48,North East,16207
2,49,North East,17441
2,50,North East,17902
2,51,North East,17216
2,52,North East,17754
2,53,North East,18064
2,54,North East,18651
2,55,North East,18771
2,56,North East,19289
2,57,North East,18819
2,58,North East,18670
2,59,North East,18501
2,60,North East,17790
2,61,North East,17227
2,62,North East,17232
2,63,North East,16987
2,64,North East,16402
2,65,North East,15538
2,66,North East,15088
2,67,North East,15095
2,68,North East,14365
2,69,North East,14086
2,70,North East,13597
2,71,North East,13899
2,72,North East,14073
2,73,North East,14985
2,74,North East,14527
2,75,North East,10757
2,76,North East,11063
2,77,North East,9540
2,78,North East,8465
2,79,North East,7489
2,80,North East,7056
2,81,North East,6855
2,82,North East,6648
2,83,North East,5870
2,84,North East,5326
2,85,North East,4515
2,86,North East,3924
2,87,North East,3378
2,88,North East,2850
2,89,North East,2413
2,90,North East,7148
1,0,North West,37534
1,1,North West,39300
1,2,North West,40177
1,3,North West,40387
1,4,North West,41630
1,5,North West,42565
1,6,North West,42448
1,7,North West,43139
1,8,North West,44204
1,9,North West,44997
1,10,North West,44791
1,11,North West,44307
1,12,North West,44202
1,13,North West,43998
1,14,North West,42791
1,15,North West,41969
1,16,North West,42032
1,17,North West,41337
1,18,North West,41242
1,19,North West,44517
1,20,North West,44048
1,21,North West,44909
1,22,North West,45810
1,23,North West,45492
1,24,North West,47242
1,25,North West,46146
1,26,North West,47157
1,27,North West,48719
1,28,North West,49761
1,29,North West,51274
1,30,North West,52928
1,31,North West,51844
1,32,North West,51909
1,33,North West,53018
1,34,North West,51164
1,35,North West,51592
1,36,North West,50578
1,37,North West,49358
1,38,North West,49102
1,39,North West,48064
1,40,North West,49022
1,41,North West,47974
1,42,North West,45045
1,43,North West,42090
1,44,North West,41918
1,45,North West,43029
1,46,North West,44077
1,47,North West,45409
1,48,North West,47942
1,49,North West,50882
1,50,North West,51897
1,51,North West,50981
1,52,North West,52184
1,53,North West,51864
1,54,North West,52301
1,55,North West,52503
1,56,North West,53472
1,57,North West,51935
1,58,North West,51398
1,59,North West,50411
1,60,North West,48049
1,61,North West,46153
1,62,North West,44508
1,63,North West,43829
1,64,North West,42108
1,65,North West,39823
1,66,North West,38613
1,67,North West,38954
1,68,North West,37558
1,69,North West,36828
1,70,North West,37634
1,71,North West,37780
1,72,North West,38593
1,73,North West,42195
1,74,North West,41102
1,75,North West,30604
1,76,North West,32019
1,77,North West,29554
1,78,North West,27752
1,79,North West,24410
1,80,North West,22942
1,81,North West,23305
1,82,North West,21793
1,83,North West,20407
1,84,North West,18737
1,85,North West,16939
1,86,North West,14963
1,87,North West,13043
1,88,North West,11595
1,89,North West,10454
1,90,North West,41001
2,0,North West,38946
2,1,North West,41209
2,2,North West,41860
2,3,North West,42149
2,4,North West,44461
2,5,North West,44863
2,6,North West,44192
2,7,North West,44871
2,8,North West,46877
2,9,North West,46918
2,10,North West,47100
2,11,North West,47053
2,12,North West,46655
2,13,North West,46308
2,14,North West,45204
2,15,North West,44659
2,16,North West,44862
2,17,North West,43198
2,18,North West,42916
2,19,North West,44221
2,20,North West,44620
2,21,North West,44757
2,22,North West,45496
2,23,North West,45181
2,24,North West,46143
2,25,North West,44855
2,26,North West,45755
2,27,North West,46834
2,28,North West,47228
2,29,North West,49096
2,30,North West,50567
2,31,North West,48874
2,32,North West,48805
2,33,North West,49641
2,34,North West,47189
2,35,North West,48560
2,36,North West,48646
2,37,North West,46947
2,38,North West,46500
2,39,North West,46274
2,40,North West,46647
2,41,North West,46679
2,42,North West,43218
2,43,North West,40992
2,44,North West,41223
2,45,North West,42256
2,46,North West,43595
2,47,North West,44287
2,48,North West,46745
2,49,North West,50147
2,50,North West,50798
2,51,North West,49805
2,52,North West,51409
2,53,North West,50672
2,54,North West,51610
2,55,North West,50947
2,56,North West,52105
2,57,North West,50772
2,58,North West,50454
2,59,North West,48510
2,60,North West,47438
2,61,North West,44993
2,62,North West,43632
2,63,North West,42758
2,64,North West,40255
2,65,North West,38471
2,66,North West,37099
2,67,North West,37623
2,68,North West,35981
2,69,North West,35003
2,70,North West,35087
2,71,North West,35177
2,72,North West,36094
2,73,North West,38460
2,74,North West,37408
2,75,North West,27505
2,76,North West,28159
2,77,North West,25976
2,78,North West,23846
2,79,North West,19997
2,80,North West,18617
2,81,North West,18425
2,82,North West,16962
2,83,North West,15524
2,84,North West,13479
2,85,North West,11971
2,86,North West,10213
2,87,North West,8388
2,88,North West,7410
2,89,North West,6098
2,90,North West,18750
1,0,Yorkshire and The Humber,27156
1,1,Yorkshire and The Humber,28749
1,2,Yorkshire and The Humber,29543
1,3,Yorkshire and The Humber,29721
1,4,Yorkshire and The Humber,30620
1,5,Yorkshire and The Humber,31126
1,6,Yorkshire and The Humber,31072
1,7,Yorkshire and The Humber,31400
1,8,Yorkshire and The Humber,32639
1,9,Yorkshire and The Humber,33218
1,10,Yorkshire and The Humber,32792
1,11,Yorkshire and The Humber,32732
1,12,Yorkshire and The Humber,32750
1,13,Yorkshire and The Humber,32253
1,14,Yorkshire and The Humber,31524
1,15,Yorkshire and The Humber,30936
1,16,Yorkshire and The Humber,30835
1,17,Yorkshire and The Humber,30387
1,18,Yorkshire and The Humber,31420
1,19,Yorkshire and The Humber,33982
1,20,Yorkshire and The Humber,34142
1,21,Yorkshire and The Humber,35782
1,22,Yorkshire and The Humber,35266
1,23,Yorkshire and The Humber,34708
1,24,Yorkshire and The Humber,34784
1,25,Yorkshire and The Humber,34012
1,26,Yorkshire and The Humber,34926
1,27,Yorkshire and The Humber,35568
1,28,Yorkshire and The Humber,36467
1,29,Yorkshire and The Humber,37365
1,30,Yorkshire and The Humber,38361
1,31,Yorkshire and The Humber,37293
1,32,Yorkshire and The Humber,37911
1,33,Yorkshire and The Humber,38383
1,34,Yorkshire and The Humber,37771
1,35,Yorkshire and The Humber,36693
1,36,Yorkshire and The Humber,36566
1,37,Yorkshire and The Humber,35493
1,38,Yorkshire and The Humber,34777
1,39,Yorkshire and The Humber,35307
1,40,Yorkshire and The Humber,35687
1,41,Yorkshire and The Humber,34827
1,42,Yorkshire and The Humber,32725
1,43,Yorkshire and The Humber,30291
1,44,Yorkshire and The Humber,30555
1,45,Yorkshire and The Humber,31557
1,46,Yorkshire and The Humber,32335
1,47,Yorkshire and The Humber,33673
1,48,Yorkshire and The Humber,35414
1,49,Yorkshire and The Humber,37822
1,50,Yorkshire and The Humber,38901
1,51,Yorkshire and The Humber,38066
1,52,Yorkshire and The Humber,38941
1,53,Yorkshire and The Humber,38624
1,54,Yorkshire and The Humber,38347
1,55,Yorkshire and The Humber,38474
1,56,Yorkshire and The Humber,38588
1,57,Yorkshire and The Humber,38255
1,58,Yorkshire and The Humber,37624
1,59,Yorkshire and The Humber,36501
1,60,Yorkshire and The Humber,35116
1,61,Yorkshire and The Humber,33624
1,62,Yorkshire and The Humber,33378
1,63,Yorkshire and The Humber,32677
1,64,Yorkshire and The Humber,31237
1,65,Yorkshire and The Humber,29702
1,66,Yorkshire and The Humber,28794
1,67,Yorkshire and The Humber,29023
1,68,Yorkshire and The Humber,27964
1,69,Yorkshire and The Humber,27510
1,70,Yorkshire and The Humber,27895
1,71,Yorkshire and The Humber,28466
1,72,Yorkshire and The Humber,28746
1,73,Yorkshire and The Humber,31823
1,74,Yorkshire and The Humber,31141
1,75,Yorkshire and The Humber,23068
1,76,Yorkshire and The Humber,24638
1,77,Yorkshire and The Humber,21628
1,78,Yorkshire and The Humber,20432
1,79,Yorkshire and The Humber,17543
1,80,Yorkshire and The Humber,16821
1,81,Yorkshire and The Humber,17347
1,82,Yorkshire and The Humber,16219
1,83,Yorkshire and The Humber,15448
1,84,Yorkshire and The Humber,14380
1,85,Yorkshire and The Humber,12845
1,86,Yorkshire and The Humber,11465
1,87,Yorkshire and The Humber,10383
1,88,Yorkshire and The Humber,9095
1,89,Yorkshire and The Humber,7894
1,90,Yorkshire and The Humber,31744
2,0,Yorkshire and The Humber,28784
2,1,Yorkshire and The Humber,30036
2,2,Yorkshire and The Humber,30639
2,3,Yorkshire and The Humber,30841
2,4,Yorkshire and The Humber,32468
2,5,Yorkshire and The Humber,32835
2,6,Yorkshire and The Humber,32316
2,7,Yorkshire and The Humber,32771
2,8,Yorkshire and The Humber,34340
2,9,Yorkshire and The Humber,34386
2,10,Yorkshire and The Humber,34752
2,11,Yorkshire and The Humber,34515
2,12,Yorkshire and The Humber,34340
2,13,Yorkshire and The Humber,33900
2,14,Yorkshire and The Humber,33077
2,15,Yorkshire and The Humber,32244
2,16,Yorkshire and The Humber,33151
2,17,Yorkshire and The Humber,32800
2,18,Yorkshire and The Humber,32373
2,19,Yorkshire and The Humber,34847
2,20,Yorkshire and The Humber,34353
2,21,Yorkshire and The Humber,35483
2,22,Yorkshire and The Humber,34767
2,23,Yorkshire and The Humber,34609
2,24,Yorkshire and The Humber,33872
2,25,Yorkshire and The Humber,33595
2,26,Yorkshire and The Humber,33614
2,27,Yorkshire and The Humber,34426
2,28,Yorkshire and The Humber,35106
2,29,Yorkshire and The Humber,35811
2,30,Yorkshire and The Humber,36618
2,31,Yorkshire and The Humber,35398
2,32,Yorkshire and The Humber,35046
2,33,Yorkshire and The Humber,35649
2,34,Yorkshire and The Humber,34911
2,35,Yorkshire and The Humber,35188
2,36,Yorkshire and The Humber,34446
2,37,Yorkshire and The Humber,34060
2,38,Yorkshire and The Humber,33533
2,39,Yorkshire and The Humber,33768
2,40,Yorkshire and The Humber,34197
2,41,Yorkshire and The Humber,33871
2,42,Yorkshire and The Humber,31698
2,43,Yorkshire and The Humber,29875
2,44,Yorkshire and The Humber,30024
2,45,Yorkshire and The Humber,31122
2,46,Yorkshire and The Humber,31497
2,47,Yorkshire and The Humber,32722
2,48,Yorkshire and The Humber,35007
2,49,Yorkshire and The Humber,36824
2,50,Yorkshire and The Humber,37567
2,51,Yorkshire and The Humber,37047
2,52,Yorkshire and The Humber,37776
2,53,Yorkshire and The Humber,37721
2,54,Yorkshire and The Humber,37618
2,55,Yorkshire and The Humber,37650
2,56,Yorkshire and The Humber,37974
2,57,Yorkshire and The Humber,36820
2,58,Yorkshire and The Humber,36669
2,59,Yorkshire and The Humber,35346
2,60,Yorkshire and The Humber,34575
2,61,Yorkshire and The Humber,32983
2,62,Yorkshire and The Humber,32108
2,63,Yorkshire and The Humber,31838
2,64,Yorkshire and The Humber,29925
2,65,Yorkshire and The Humber,29029
2,66,Yorkshire and The Humber,27456
2,67,Yorkshire and The Humber,28261
2,68,Yorkshire and The Humber,26820
2,69,Yorkshire and The Humber,26149
2,70,Yorkshire and The Humber,26297
2,71,Yorkshire and The Humber,26273
2,72,Yorkshire and The Humber,26765
2,73,Yorkshire and The Humber,28874
2,74,Yorkshire and The Humber,28170
2,75,Yorkshire and The Humber,20406
2,76,Yorkshire and The Humber,21706
2,77,Yorkshire and The Humber,19432
2,78,Yorkshire and The Humber,17659
2,79,Yorkshire and The Humber,14614
2,80,Yorkshire and The Humber,13521
2,81,Yorkshire and The Humber,13602
2,82,Yorkshire and The Humber,12876
2,83,Yorkshire and The Humber,11812
2,84,Yorkshire and The Humber,10460
2,85,Yorkshire and The Humber,9146
2,86,Yorkshire and The Humber,7904
2,87,Yorkshire and The Humber,6741
2,88,Yorkshire and The Humber,5703
2,89,Yorkshire and The Humber,4658
2,90,Yorkshire and The Humber,14637
1,0,East Midlands,22668
1,1,East Midlands,24167
1,2,East Midlands,24694
1,3,East Midlands,25259
1,4,East Midlands,26300
1,5,East Midlands,27235
1,6,East Midlands,26821
1,7,East Midlands,27225
1,8,East Midlands,28473
1,9,East Midlands,28983
1,10,East Midlands,29021
1,11,East Midlands,28332
1,12,East Midlands,28546
1,13,East Midlands,28169
1,14,East Midlands,27517
1,15,East Midlands,26846
1,16,East Midlands,26490
1,17,East Midlands,25971
1,18,East Midlands,28163
1,19,East Midlands,32425
1,20,East Midlands,32532
1,21,East Midlands,31476
1,22,East Midlands,29840
1,23,East Midlands,28800
1,24,East Midlands,28647
1,25,East Midlands,28509
1,26,East Midlands,29770
1,27,East Midlands,30532
1,28,East Midlands,31012
1,29,East Midlands,32197
1,30,East Midlands,33073
1,31,East Midlands,32652
1,32,East Midlands,32505
1,33,East Midlands,33108
1,34,East Midlands,32364
1,35,East Midlands,32081
1,36,East Midlands,32164
1,37,East Midlands,31304
1,38,East Midlands,31346
1,39,East Midlands,30656
1,40,East Midlands,32063
1,41,East Midlands,31196
1,42,East Midlands,29772
1,43,East Midlands,27629
1,44,East Midlands,27635
1,45,East Midlands,28838
1,46,East Midlands,29856
1,47,East Midlands,30661
1,48,East Midlands,32624
1,49,East Midlands,34204
1,50,East Midlands,35228
1,51,East Midlands,34436
1,52,East Midlands,35551
1,53,East Midlands,34840
1,54,East Midlands,35188
1,55,East Midlands,35269
1,56,East Midlands,35464
1,57,East Midlands,34515
1,58,East Midlands,34005
1,59,East Midlands,33004
1,60,East Midlands,31626
1,61,East Midlands,30264
1,62,East Midlands,29747
1,63,East Midlands,28805
1,64,East Midlands,27191
1,65,East Midlands,26417
1,66,East Midlands,26028
1,67,East Midlands,26479
1,68,East Midlands,25808
1,69,East Midlands,25393
1,70,East Midlands,25104
1,71,East Midlands,26486
1,72,East Midlands,26677
1,73,East Midlands,28991
1,74,East Midlands,27886
1,75,East Midlands,21473
1,76,East Midlands,23023
1,77,East Midlands,21154
1,78,East Midlands,18989
1,79,East Midlands,16126
1,80,East Midlands,14981
1,81,East Midlands,15177
1,82,East Midlands,14678
1,83,East Midlands,13486
1,84,East Midlands,12294
1,85,East Midlands,11221
1,86,East Midlands,9959
1,87,East Midlands,8966
1,88,East Midlands,7829
1,89,East Midlands,6911
1,90,East Midlands,28844
2,0,East Midlands,24212
2,1,East Midlands,25412
2,2,East Midlands,25970
2,3,East Midlands,26748
2,4,East Midlands,27768
2,5,East Midlands,28106
2,6,East Midlands,28286
2,7,East Midlands,28261
2,8,East Midlands,29564
2,9,East Midlands,30362
2,10,East Midlands,29930
2,11,East Midlands,29813
2,12,East Midlands,29856
2,13,East Midlands,30452
2,14,East Midlands,28704
2,15,East Midlands,28307
2,16,East Midlands,28329
2,17,East Midlands,27990
2,18,East Midlands,29571
2,19,East Midlands,33166
2,20,East Midlands,33093
2,21,East Midlands,32734
2,22,East Midlands,31024
2,23,East Midlands,29221
2,24,East Midlands,29051
2,25,East Midlands,28695
2,26,East Midlands,29472
2,27,East Midlands,29503
2,28,East Midlands,30434
2,29,East Midlands,31041
2,30,East Midlands,31764
2,31,East Midlands,31190
2,32,East Midlands,30856
2,33,East Midlands,31506
2,34,East Midlands,30116
2,35,East Midlands,30671
2,36,East Midlands,30242
2,37,East Midlands,29817
2,38,East Midlands,29900
2,39,East Midlands,29852
2,40,East Midlands,30961
2,41,East Midlands,30160
2,42,East Midlands,28474
2,43,East Midlands,26878
2,44,East Midlands,27495
2,45,East Midlands,27997
2,46,East Midlands,29174
2,47,East Midlands,29726
2,48,East Midlands,31683
2,49,East Midlands,33608
2,50,East Midlands,34265
2,51,East Midlands,33289
2,52,East Midlands,34613
2,53,East Midlands,34581
2,54,East Midlands,34568
2,55,East Midlands,34488
2,56,East Midlands,34438
2,57,East Midlands,33604
2,58,East Midlands,33265
2,59,East Midlands,31976
2,60,East Midlands,31466
2,61,East Midlands,29538
2,62,East Midlands,28828
2,63,East Midlands,27844
2,64,East Midlands,26881
2,65,East Midlands,25462
2,66,East Midlands,24772
2,67,East Midlands,25166
2,68,East Midlands,24206
2,69,East Midlands,24087
2,70,East Midlands,24214
2,71,East Midlands,24397
2,72,East Midlands,24325
2,73,East Midlands,26504
2,74,East Midlands,26287
2,75,East Midlands,19753
2,76,East Midlands,20661
2,77,East Midlands,18711
2,78,East Midlands,16955
2,79,East Midlands,13968
2,80,East Midlands,12787
2,81,East Midlands,12403
2,82,East Midlands,11730
2,83,East Midlands,10660
2,84,East Midlands,9266
2,85,East Midlands,8153
2,86,East Midlands,7156
2,87,East Midlands,6085
2,88,East Midlands,5333
2,89,East Midlands,4548
2,90,East Midlands,13812
1,0,West Midlands,31054
1,1,West Midlands,32152
1,2,West Midlands,32815
1,3,West Midlands,33399
1,4,West Midlands,34636
1,5,West Midlands,34846
1,6,West Midlands,34704
1,7,West Midlands,35624
1,8,West Midlands,36940
1,9,West Midlands,37346
1,10,West Midlands,37072
1,11,West Midlands,36307
1,12,West Midlands,36757
1,13,West Midlands,36697
1,14,West Midlands,35646
1,15,West Midlands,34590
1,16,West Midlands,35120
1,17,West Midlands,34322
1,18,West Midlands,33962
1,19,West Midlands,35645
1,20,West Midlands,35083
1,21,West Midlands,35213
1,22,West Midlands,35957
1,23,West Midlands,35939
1,24,West Midlands,36716
1,25,West Midlands,36020
1,26,West Midlands,36781
1,27,West Midlands,37839
1,28,West Midlands,38634
1,29,West Midlands,39878
1,30,West Midlands,41348
1,31,West Midlands,41097
1,32,West Midlands,41035
1,33,West Midlands,41817
1,34,West Midlands,40997
1,35,West Midlands,40181
1,36,West Midlands,39782
1,37,West Midlands,39338
1,38,West Midlands,39331
1,39,West Midlands,38412
1,40,West Midlands,39774
1,41,West Midlands,39174
1,42,West Midlands,36357
1,43,West Midlands,33974
1,44,West Midlands,34291
1,45,West Midlands,34658
1,46,West Midlands,35601
1,47,West Midlands,36514
1,48,West Midlands,38220
1,49,West Midlands,40876
1,50,West Midlands,41672
1,51,West Midlands,41023
1,52,West Midlands,41799
1,53,West Midlands,41498
1,54,West Midlands,41506
1,55,West Midlands,41502
1,56,West Midlands,41351
1,57,West Midlands,39748
1,58,West Midlands,39266
1,59,West Midlands,38427
1,60,West Midlands,36870
1,61,West Midlands,34715
1,62,West Midlands,34066
1,63,West Midlands,33422
1,64,West Midlands,32170
1,65,West Midlands,30606
1,66,West Midlands,30295
1,67,West Midlands,30770
1,68,West Midlands,29474
1,69,West Midlands,29052
1,70,West Midlands,29090
1,71,West Midlands,29987
1,72,West Midlands,30102
1,73,West Midlands,32265
1,74,West Midlands,31590
1,75,West Midlands,24901
1,76,West Midlands,26796
1,77,West Midlands,25428
1,78,West Midlands,23339
1,79,West Midlands,19889
1,80,West Midlands,18690
1,81,West Midlands,18508
1,82,West Midlands,18008
1,83,West Midlands,16624
1,84,West Midlands,15720
1,85,West Midlands,14040
1,86,West Midlands,12389
1,87,West Midlands,11151
1,88,West Midlands,10148
1,89,West Midlands,8987
1,90,West Midlands,35829
2,0,West Midlands,32114
2,1,West Midlands,33697
2,2,West Midlands,34575
2,3,West Midlands,34682
2,4,West Midlands,36266
2,5,West Midlands,36763
2,6,West Midlands,36639
2,7,West Midlands,37298
2,8,West Midlands,39198
2,9,West Midlands,38942
2,10,West Midlands,39062
2,11,West Midlands,38578
2,12,West Midlands,38750
2,13,West Midlands,38488
2,14,West Midlands,37060
2,15,West Midlands,36731
2,16,West Midlands,37518
2,17,West Midlands,36845
2,18,West Midlands,36090
2,19,West Midlands,36661
2,20,West Midlands,36715
2,21,West Midlands,37489
2,22,West Midlands,37231
2,23,West Midlands,36752
2,24,West Midlands,36325
2,25,West Midlands,36110
2,26,West Midlands,36017
2,27,West Midlands,36586
2,28,West Midlands,36736
2,29,West Midlands,38402
2,30,West Midlands,39548
2,31,West Midlands,38818
2,32,West Midlands,38357
2,33,West Midlands,38941
2,34,West Midlands,37978
2,35,West Midlands,37970
2,36,West Midlands,37958
2,37,West Midlands,36981
2,38,West Midlands,36545
2,39,West Midlands,36641
2,40,West Midlands,37855
2,41,West Midlands,36919
2,42,West Midlands,35454
2,43,West Midlands,33268
2,44,West Midlands,33365
2,45,West Midlands,34317
2,46,West Midlands,35491
2,47,West Midlands,36157
2,48,West Midlands,38378
2,49,West Midlands,39946
2,50,West Midlands,41088
2,51,West Midlands,40505
2,52,West Midlands,41630
2,53,West Midlands,40865
2,54,West Midlands,41159
2,55,West Midlands,40207
2,56,West Midlands,39908
2,57,West Midlands,38894
2,58,West Midlands,38408
2,59,West Midlands,37816
2,60,West Midlands,36292
2,61,West Midlands,34271
2,62,West Midlands,33680
2,63,West Midlands,32934
2,64,West Midlands,31152
2,65,West Midlands,29487
2,66,West Midlands,29120
2,67,West Midlands,29251
2,68,West Midlands,28237
2,69,West Midlands,27463
2,70,West Midlands,27098
2,71,West Midlands,27679
2,72,West Midlands,28154
2,73,West Midlands,29226
2,74,West Midlands,28669
2,75,West Midlands,22505
2,76,West Midlands,23552
2,77,West Midlands,22300
2,78,West Midlands,19879
2,79,West Midlands,16934
2,80,West Midlands,15325
2,81,West Midlands,15287
2,82,West Midlands,14345
2,83,West Midlands,12818
2,84,West Midlands,11635
2,85,West Midlands,10025
2,86,West Midlands,8741
2,87,West Midlands,7245
2,88,West Midlands,6350
2,89,West Midlands,5346
2,90,West Midlands,16876
1,0,East of England,31663
1,1,East of England,33284
1,2,East of England,34275
1,3,East of England,34798
1,4,East of England,36019
1,5,East of England,36749
1,6,East of England,36098
1,7,East of England,36699
1,8,East of England,38099
1,9,East of England,38237
1,10,East of England,38091
1,11,East of England,38245
1,12,East of England,37986
1,13,East of England,37110
1,14,East of England,36135
1,15,East of England,35000
1,16,East of England,34671
1,17,East of England,34833
1,18,East of England,31472
1,19,East of England,29697
1,20,East of England,29434
1,21,East of England,31142
1,22,East of England,34610
1,23,East of England,35817
1,24,East of England,36260
1,25,East of England,36906
1,26,East of England,37929
1,27,East of England,39548
1,28,East of England,40433
1,29,East of England,42154
1,30,East of England,43193
1,31,East of England,43329
1,32,East of England,44103
1,33,East of England,44829
1,34,East of England,43774
1,35,East of England,43537
1,36,East of England,43571
1,37,East of England,42812
1,38,East of England,42228
1,39,East of England,43369
1,40,East of England,44410
1,41,East of England,43617
1,42,East of England,40941
1,43,East of England,38640
1,44,East of England,38942
1,45,East of England,39333
1,46,East of England,40690
1,47,East of England,41202
1,48,East of England,42941
1,49,East of England,44090
1,50,East of England,45220
1,51,East of England,43608
1,52,East of England,45409
1,53,East of England,45197
1,54,East of England,45464
1,55,East of England,45658
1,56,East of England,45272
1,57,East of England,44412
1,58,East of England,42874
1,59,East of England,41521
1,60,East of England,40176
1,61,East of England,37818
1,62,East of England,37652
1,63,East of England,36360
1,64,East of England,34992
1,65,East of England,33976
1,66,East of England,33469
1,67,East of England,33354
1,68,East of England,32708
1,69,East of England,31984
1,70,East of England,32376
1,71,East of England,33307
1,72,East of England,34169
1,73,East of England,37791
1,74,East of England,38944
1,75,East of England,28113
1,76,East of England,29652
1,77,East of England,26817
1,78,East of England,25181
1,79,East of England,21272
1,80,East of England,19834
1,81,East of England,20274
1,82,East of England,19610
1,83,East of England,18333
1,84,East of England,16845
1,85,East of England,15659
1,86,East of England,13934
1,87,East of England,12356
1,88,East of England,11301
1,89,East of England,10173
1,90,East of England,42084
2,0,East of England,33147
2,1,East of England,35288
2,2,East of England,36287
2,3,East of England,36854
2,4,East of England,37529
2,5,East of England,38721
2,6,East of England,38026
2,7,East of England,38170
2,8,East of England,39801
2,9,East of England,40018
2,10,East of England,40626
2,11,East of England,39670
2,12,East of England,39482
2,13,East of England,39672
2,14,East of England,38059
2,15,East of England,36952
2,16,East of England,37135
2,17,East of England,37005
2,18,East of England,34314
2,19,East of England,31953
2,20,East of England,31787
2,21,East of England,33648
2,22,East of England,35529
2,23,East of England,36858
2,24,East of England,37288
2,25,East of England,36826
2,26,East of England,37526
2,27,East of England,38344
2,28,East of England,38860
2,29,East of England,39414
2,30,East of England,40956
2,31,East of England,40691
2,32,East of England,41078
2,33,East of England,41716
2,34,East of England,40742
2,35,East of England,41144
2,36,East of England,40158
2,37,East of England,40162
2,38,East of England,40038
2,39,East of England,40877
2,40,East of England,42098
2,41,East of England,41289
2,42,East of England,40154
2,43,East of England,37560
2,44,East of England,38192
2,45,East of England,38183
2,46,East of England,39391
2,47,East of England,40506
2,48,East of England,41283
2,49,East of England,43190
2,50,East of England,43689
2,51,East of England,42849
2,52,East of England,43743
2,53,East of England,43477
2,54,East of England,44178
2,55,East of England,44010
2,56,East of England,43618
2,57,East of England,42660
2,58,East of England,41761
2,59,East of England,40708
2,60,East of England,39339
2,61,East of England,37335
2,62,East of England,36571
2,63,East of England,35532
2,64,East of England,33701
2,65,East of England,32183
2,66,East of England,31192
2,67,East of England,30948
2,68,East of England,30023
2,69,East of England,29386
2,70,East of England,29581
2,71,East of England,30252
2,72,East of England,30974
2,73,East of England,34284
2,74,East of England,34306
2,75,East of England,25489
2,76,East of England,26537
2,77,East of England,23723
2,78,East of England,21940
2,79,East of England,17837
2,80,East of England,16207
2,81,East of England,16564
2,82,East of England,15670
2,83,East of England,14614
2,84,East of England,13312
2,85,East of England,11289
2,86,East of England,10153
2,87,East of England,8670
2,88,East of England,7436
2,89,East of England,6404
2,90,East of England,20632
1,0,London,51365
1,1,London,52461
1,2,London,51422
1,3,London,51772
1,4,London,52344
1,5,London,51978
1,6,London,50743
1,7,London,51056
1,8,London,52906
1,9,London,53367
1,10,London,52943
1,11,London,52981
1,12,London,53016
1,13,London,52255
1,14,London,50568
1,15,London,48872
1,16,London,49725
1,17,London,49107
1,18,London,47062
1,19,London,45495
1,20,London,47423
1,21,London,52773
1,22,London,60115
1,23,London,69250
1,24,London,74886
1,25,London,78963
1,26,London,81084
1,27,London,82627
1,28,London,84296
1,29,London,85243
1,30,London,85988
1,31,London,85726
1,32,London,85317
1,33,London,84173
1,34,London,81222
1,35,London,80312
1,36,London,77993
1,37,London,75970
1,38,London,74846
1,39,London,73606
1,40,London,74126
1,41,London,72435
1,42,London,68385
1,43,London,64728
1,44,London,63451
1,45,London,61785
1,46,London,61694
1,47,London,59538
1,48,London,59568
1,49,London,58620
1,50,London,60891
1,51,London,59019
1,52,London,59336
1,53,London,58168
1,54,London,56428
1,55,London,56592
1,56,London,55117
1,57,London,53039
1,58,London,51102
1,59,London,48073
1,60,London,46511
1,61,London,43571
1,62,London,41603
1,63,London,38598
1,64,London,37376
1,65,London,35105
1,66,London,33353
1,67,London,31786
1,68,London,30915
1,69,London,29408
1,70,London,29140
1,71,London,28817
1,72,London,28437
1,73,London,29349
1,74,London,28881
1,75,London,23288
1,76,London,22816
1,77,London,21361
1,78,London,20544
1,79,London,17680
1,80,London,17119
1,81,London,17495
1,82,London,16692
1,83,London,15360
1,84,London,14145
1,85,London,13049
1,86,London,11717
1,87,London,10111
1,88,London,9311
1,89,London,8170
1,90,London,34566
2,0,London,52867
2,1,London,54678
2,2,London,54274
2,3,London,53488
2,4,London,54287
2,5,London,54787
2,6,London,53387
2,7,London,53156
2,8,London,54762
2,9,London,55353
2,10,London,55854
2,11,London,55190
2,12,London,54517
2,13,London,54707
2,14,London,53409
2,15,London,50977
2,16,London,52219
2,17,London,51271
2,18,London,48499
2,19,London,46159
2,20,London,47094
2,21,London,50217
2,22,London,56306
2,23,London,62704
2,24,London,68419
2,25,London,71247
2,26,London,73671
2,27,London,75473
2,28,London,76331
2,29,London,77612
2,30,London,79082
2,31,London,78576
2,32,London,78234
2,33,London,76750
2,34,London,73272
2,35,London,73441
2,36,London,71871
2,37,London,69695
2,38,London,69108
2,39,London,68865
2,40,London,69212
2,41,London,67727
2,42,London,64890
2,43,London,61597
2,44,London,60729
2,45,London,60098
2,46,London,59842
2,47,London,58000
2,48,London,57753
2,49,London,57072
2,50,London,57462
2,51,London,56620
2,52,London,56419
2,53,London,55241
2,54,London,54208
2,55,London,53147
2,56,London,51816
2,57,London,50086
2,58,London,47627
2,59,London,45015
2,60,London,44246
2,61,London,41552
2,62,London,38968
2,63,London,37239
2,64,London,34921
2,65,London,32785
2,66,London,30670
2,67,London,28841
2,68,London,27279
2,69,London,26200
2,70,London,25664
2,71,London,24769
2,72,London,24383
2,73,London,25084
2,74,London,24611
2,75,London,18854
2,76,London,18751
2,77,London,17045
2,78,London,15764
2,79,London,13561
2,80,London,12981
2,81,London,13264
2,82,London,12280
2,83,London,11113
2,84,London,10214
2,85,London,8912
2,86,London,7819
2,87,London,6863
2,88,London,5916
2,89,London,4938
2,90,London,16238
1,0,South East,44565
1,1,South East,46971
1,2,South East,48703
1,3,South East,49622
1,4,South East,51313
1,5,South East,52914
1,6,South East,52107
1,7,South East,52580
1,8,South East,54996
1,9,South East,56295
1,10,South East,56215
1,11,South East,55872
1,12,South East,55362
1,13,South East,55477
1,14,South East,54072
1,15,South East,51850
1,16,South East,52328
1,17,South East,51527
1,18,South East,49187
1,19,South East,48665
1,20,South East,47907
1,21,South East,49719
1,22,South East,51943
1,23,South East,52529
1,24,South East,52102
1,25,South East,52245
1,26,South East,53562
1,27,South East,55175
1,28,South East,57085
1,29,South East,58921
1,30,South East,61653
1,31,South East,61705
1,32,South East,62830
1,33,South East,63576
1,34,South East,63051
1,35,South East,63022
1,36,South East,62601
1,37,South East,62413
1,38,South East,62591
1,39,South East,63273
1,40,South East,65505
1,41,South East,64736
1,42,South East,61518
1,43,South East,58778
1,44,South East,58939
1,45,South East,59283
1,46,South East,60889
1,47,South East,61840
1,48,South East,64148
1,49,South East,66113
1,50,South East,67375
1,51,South East,64977
1,52,South East,67088
1,53,South East,67273
1,54,South East,67756
1,55,South East,67109
1,56,South East,67273
1,57,South East,65770
1,58,South East,63811
1,59,South East,61251
1,60,South East,59078
1,61,South East,56216
1,62,South East,55678
1,63,South East,53252
1,64,South East,50972
1,65,South East,48871
1,66,South East,48625
1,67,South East,47917
1,68,South East,46833
1,69,South East,46787
1,70,South East,46630
1,71,South East,48258
1,72,South East,49756
1,73,South East,54410
1,74,South East,55066
1,75,South East,41257
1,76,South East,43197
1,77,South East,39731
1,78,South East,36985
1,79,South East,31050
1,80,South East,29111
1,81,South East,30267
1,82,South East,28710
1,83,South East,26847
1,84,South East,24655
1,85,South East,22575
1,86,South East,20709
1,87,South East,18446
1,88,South East,17039
1,89,South East,15201
1,90,South East,64743
2,0,South East,47141
2,1,South East,49414
2,2,South East,51439
2,3,South East,52290
2,4,South East,53843
2,5,South East,55176
2,6,South East,54683
2,7,South East,55678
2,8,South East,58412
2,9,South East,59405
2,10,South East,59541
2,11,South East,58603
2,12,South East,58695
2,13,South East,58331
2,14,South East,56825
2,15,South East,55094
2,16,South East,55183
2,17,South East,55539
2,18,South East,53152
2,19,South East,51524
2,20,South East,50415
2,21,South East,52157
2,22,South East,53700
2,23,South East,53289
2,24,South East,52708
2,25,South East,52562
2,26,South East,53257
2,27,South East,53241
2,28,South East,54198
2,29,South East,55760
2,30,South East,57528
2,31,South East,57436
2,32,South East,58218
2,33,South East,59600
2,34,South East,57968
2,35,South East,58667
2,36,South East,58488
2,37,South East,58241
2,38,South East,58999
2,39,South East,59432
2,40,South East,61547
2,41,South East,61555
2,42,South East,58885
2,43,South East,56429
2,44,South East,56494
2,45,South East,57097
2,46,South East,59068
2,47,South East,59954
2,48,South East,62473
2,49,South East,63852
2,50,South East,64700
2,51,South East,63451
2,52,South East,64340
2,53,South East,64409
2,54,South East,65732
2,55,South East,65243
2,56,South East,65398
2,57,South East,63752
2,58,South East,61834
2,59,South East,59653
2,60,South East,57796
2,61,South East,54811
2,62,South East,53717
2,63,South East,51703
2,64,South East,48932
2,65,South East,47013
2,66,South East,45574
2,67,South East,44835
2,68,South East,43456
2,69,South East,42361
2,70,South East,42086
2,71,South East,43018
2,72,South East,44306
2,73,South East,49171
2,74,South East,48849
2,75,South East,36430
2,76,South East,37877
2,77,South East,34243
2,78,South East,31032
2,79,South East,25885
2,80,South East,24058
2,81,South East,24192
2,82,South East,22260
2,83,South East,20554
2,84,South East,18719
2,85,South East,16643
2,86,South East,14543
2,87,South East,12508
2,88,South East,10819
2,89,South East,9464
2,90,South East,30687
1,0,South West,24953
1,1,South West,25800
1,2,South West,26890
1,3,South West,27557
1,4,South West,28374
1,5,South West,29494
1,6,South West,29629
1,7,South West,29792
1,8,South West,31018
1,9,South West,31702
1,10,South West,31696
1,11,South West,31545
1,12,South West,31228
1,13,South West,31159
1,14,South West,30750
1,15,South West,29409
1,16,South West,29805
1,17,South West,29798
1,18,South West,30999
1,19,South West,32882
1,20,South West,32865
1,21,South West,32901
1,22,South West,32935
1,23,South West,32148
1,24,South West,31880
1,25,South West,31482
1,26,South West,32866
1,27,South West,33469
1,28,South West,33986
1,29,South West,34989
1,30,South West,36228
1,31,South West,36504
1,32,South West,36551
1,33,South West,37084
1,34,South West,35900
1,35,South West,35327
1,36,South West,35471
1,37,South West,34968
1,38,South West,34507
1,39,South West,34907
1,40,South West,35877
1,41,South West,35169
1,42,South West,33471
1,43,South West,31565
1,44,South West,31641
1,45,South West,32640
1,46,South West,34149
1,47,South West,35228
1,48,South West,37101
1,49,South West,39270
1,50,South West,39932
1,51,South West,39816
1,52,South West,40504
1,53,South West,40893
1,54,South West,41896
1,55,South West,42514
1,56,South West,42419
1,57,South West,41854
1,58,South West,41203
1,59,South West,39832
1,60,South West,38899
1,61,South West,37463
1,62,South West,37167
1,63,South West,36479
1,64,South West,35024
1,65,South West,33495
1,66,South West,33591
1,67,South West,34417
1,68,South West,33208
1,69,South West,33202
1,70,South West,32884
1,71,South West,34435
1,72,South West,35093
1,73,South West,38379
1,74,South West,37924
1,75,South West,29094
1,76,South West,30677
1,77,South West,28109
1,78,South West,25809
1,79,South West,22176
1,80,South West,20409
1,81,South West,20761
1,82,South West,19573
1,83,South West,18261
1,84,South West,17056
1,85,South West,15403
1,86,South West,14268
1,87,South West,12613
1,88,South West,11723
1,89,South West,10549
1,90,South West,44990
2,0,South West,26045
2,1,South West,27474
2,2,South West,28530
2,3,South West,29059
2,4,South West,29764
2,5,South West,31095
2,6,South West,30514
2,7,South West,31322
2,8,South West,32900
2,9,South West,33607
2,10,South West,33327
2,11,South West,32835
2,12,South West,32450
2,13,South West,32839
2,14,South West,31797
2,15,South West,30793
2,16,South West,31118
2,17,South West,31804
2,18,South West,32503
2,19,South West,34532
2,20,South West,34361
2,21,South West,34450
2,22,South West,34502
2,23,South West,33353
2,24,South West,32805
2,25,South West,32671
2,26,South West,32563
2,27,South West,33012
2,28,South West,33198
2,29,South West,33935
2,30,South West,35080
2,31,South West,34809
2,32,South West,34647
2,33,South West,35070
2,34,South West,34068
2,35,South West,34171
2,36,South West,33644
2,37,South West,33714
2,38,South West,33376
2,39,South West,33958
2,40,South West,34380
2,41,South West,34016
2,42,South West,32473
2,43,South West,30320
2,44,South West,30559
2,45,South West,31188
2,46,South West,32447
2,47,South West,34157
2,48,South West,35798
2,49,South West,37291
2,50,South West,38050
2,51,South West,37946
2,52,South West,39033
2,53,South West,39168
2,54,South West,39978
2,55,South West,40392
2,56,South West,40670
2,57,South West,40267
2,58,South West,39122
2,59,South West,38584
2,60,South West,37599
2,61,South West,35831
2,62,South West,35572
2,63,South West,34271
2,64,South West,33299
2,65,South West,31786
2,66,South West,31281
2,67,South West,31673
2,68,South West,30580
2,69,South West,30542
2,70,South West,30506
2,71,South West,31371
2,72,South West,32153
2,73,South West,35044
2,74,South West,35113
2,75,South West,26045
2,76,South West,27747
2,77,South West,24492
2,78,South West,23226
2,79,South West,18940
2,80,South West,17170
2,81,South West,17131
2,82,South West,16004
2,83,South West,14589
2,84,South West,13362
2,85,South West,11608
2,86,South West,10333
2,87,South West,8759
2,88,South West,7705
2,89,South West,6503
2,90,South West,21873
"""
pop_census2021_region = spark.createDataFrame(pd.DataFrame(pd.read_csv(io.StringIO(pop_census2021_region))))

# COMMAND ----------

display(pop_census2021_region)

# COMMAND ----------

# Check region names in both the population estimate source and the cohort source
#tab(pop_census2021_region, 'region'); print()
#tab(_cohort, 'region'); print()

# People in region like Scotland, Wales or None have now been excluded from the cohort

# COMMAND ----------

# MAGIC %md # 3. Calculate weights

# COMMAND ----------

# MAGIC %md ## 3.1. Calculate age-sex weights

# COMMAND ----------

# summarise by sex and age
tmp_w1 = _cohort\
  .withColumn('AGE', f.when(f.col('AGE') > 90, 90).otherwise(f.col('AGE')))\
  .groupBy('SEX', 'AGE')\
  .agg(f.count(f.lit(1)).alias('cohort'))\
  .orderBy('SEX', 'AGE')

# add pop est
tmp_w2 = merge(tmp_w1, pop, ['SEX', 'AGE'], validate='1:1', indicator=0); print()
tmp_w3 = merge(tmp_w2, pop_census2021, ['SEX', 'AGE'], validate='1:1', indicator=0); print()

# compare
tmp_w4 = tmp_w3\
  .withColumn('weight_pe_mid2020', f.col('pe_mid2020')/f.col('cohort'))\
  .withColumn('weight_pe_census2021', f.col('pe_census2021')/f.col('cohort'))\
  .where(~f.col('cohort').isNull())

# COMMAND ----------

display(tmp_w4.orderBy('SEX', 'AGE'))

# COMMAND ----------

outName = f'{proj}_tmp_population_estimates_age_sex'
tmp_w4.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
tmp_w4 = spark.table(f'{dbc}.{outName}')   

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(pe_mid2020) AS sum_pe_mid2020, sum(pe_census2021) AS sum_pe_census2021, sum(cohort) AS sum_cohort
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu051_tmp_population_estimates_age_sex

# COMMAND ----------

# MAGIC %md ## 3.2. Calculate age-sex-region weights

# COMMAND ----------

#Merge age-sex cohort-region counts with population estimates

# summarise by sex, age and region
tmp_region = _cohort\
  .withColumn('AGE', f.when(f.col('AGE') > 90, 90).otherwise(f.col('AGE')))\
  .groupBy('SEX', 'AGE','region')\
  .agg(f.count(f.lit(1)).alias('cohort'))\
  .orderBy('SEX', 'AGE','region')

# add pop est
tmp2_region = merge(tmp_region, pop_census2021_region, ['SEX', 'AGE','region'], validate='1:1', indicator=0); print()

# compare
tmp3_region = tmp2_region\
  .withColumn('weight_pe_census2021_region', f.col('pe_census2021')/f.col('cohort'))\
  .where(~f.col('cohort').isNull())

# COMMAND ----------

display(tmp3_region.orderBy('SEX', 'AGE','region'))

# COMMAND ----------

outName = f'{proj}_tmp_population_estimates_age_sex_region'
tmp3_region.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
tmp3_region = spark.table(f'{dbc}.{outName}')   

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(pe_census2021) as sum_pe_census2021_region, sum(cohort) as sum_cohort FROM dars_nic_391419_j3w9t_collab.ccu051_tmp_population_estimates_age_sex_region

# COMMAND ----------

# MAGIC %md # 4. Identify contact with health services in the last 5 years

# COMMAND ----------

# MAGIC %md ## 4.1. Identify

# COMMAND ----------

list_datasets = ['deaths', 'gdppr', 'hes_apc', 'hes_op', 'hes_ae', 'vacc', 'sgss', 'pmeds']

tmpfm = []
for i, dataset in enumerate(list_datasets):
  print(i, dataset)

  # ----------------------------------------------------------------------------
  # get id var and date var
  # ----------------------------------------------------------------------------
  row = parameters_df_datasets[parameters_df_datasets['dataset'] == dataset]
  
  # check one row only
  assert row.shape[0] != 0, f"dataset = {dataset} not found in parameters_df_datasets (datasets = {parameters_df_datasets['dataset'].tolist()})"
  assert row.shape[0] == 1, f"dataset = {dataset} has >1 row in parameters_df_datasets"
  
  idvar = row.iloc[0]['idVar']
  datevar = row.iloc[0]['dateVar']
  
  if(dataset == 'gdppr'): datevar = 'RECORD_DATE'
  
  # check
  print('  ', idvar, datevar)
  
  # ----------------------------------------------------------------------------
  # get data
  # ----------------------------------------------------------------------------
  tmpf1 = extract_batch_from_archive(parameters_df_datasets, row.iloc[0]['dataset'])
  
  # ----------------------------------------------------------------------------
  # summarise data (2 years)
  # ----------------------------------------------------------------------------
  tmpf1_2y = (
    tmpf1
    .select(f.col(idvar).alias('PERSON_ID'), f.col(datevar).alias('DATE'))
    .where(f.col('PERSON_ID').isNotNull())
    .where(f.col('DATE').isNotNull())
    .where(f.col('DATE') >= f.to_date(f.lit('2020-06-01')))
    .where(f.col('DATE') < f.to_date(f.lit('2022-12-31')))
    .groupBy('PERSON_ID')
    .agg(f.max('DATE').alias(f'max_date_{dataset}'))
    .withColumn(f'in_{dataset}', f.lit(1))
  )

  if(i == 0): tmpfm_2y = tmpf1_2y
  else:
    tmpfm_2y = (
      tmpfm_2y
      .join(tmpf1_2y, on=['PERSON_ID'], how='outer')
    )
    
  # ----------------------------------------------------------------------------
  # summarise data (5 years)
  # ----------------------------------------------------------------------------
  tmpf1 = (
    tmpf1
    .select(f.col(idvar).alias('PERSON_ID'), f.col(datevar).alias('DATE'))
    .where(f.col('PERSON_ID').isNotNull())
    .where(f.col('DATE').isNotNull())
    .where(f.col('DATE') >= f.to_date(f.lit('2017-06-01')))
    .where(f.col('DATE') < f.to_date(f.lit('2022-12-31')))
    .groupBy('PERSON_ID')
    .agg(f.max('DATE').alias(f'max_date_{dataset}'))
    .withColumn(f'in_{dataset}', f.lit(1))
  )

  if(i == 0): tmpfm = tmpf1
  else:
    tmpfm = (
      tmpfm
      .join(tmpf1, on=['PERSON_ID'], how='outer')
    )            

# COMMAND ----------

# save
outName = f'{proj}_tmp_sample_weights_contact_last_2_years'
tmpfm_2y.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
tmpfm_2y = spark.table(f'{dbc}.{outName}')    

# COMMAND ----------

# save
outName = f'{proj}_tmp_sample_weights_contact_last_5_years'
tmpfm.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
tmpfm = spark.table(f'{dbc}.{outName}')    

# COMMAND ----------

# MAGIC %md ## 4.2. Check

# COMMAND ----------

display(tmpfm_2y)

# COMMAND ----------

display(tmpfm)

# COMMAND ----------

# check
count_var(tmpfm_2y, 'PERSON_ID'); print()

# COMMAND ----------

# check
count_var(tmpfm, 'PERSON_ID'); print()

# COMMAND ----------

# MAGIC %md ## 4.3. Merge with cohort

# COMMAND ----------

tmpg1_2y = merge(_cohort, tmpfm_2y, ['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'], indicator=0); print()

# Create variable contact with health services in the last 5 years
tmpg2_2y = tmpg1_2y\
  .fillna(value=0, subset=[col for col in tmpfm.columns if re.match('^in_', col)])\
  .withColumn('_concat', f.concat(*[f.col(col) for col in tmpfm.columns if re.match('^in_', col)]))\
  .withColumn('contact_health_services_2y', f.when(f.col('_concat') != '00000000', 1).otherwise(0))

# check
tmpt = tab(tmpg2_2y, 'contact_health_services_2y'); print()
tmpt = tab(tmpg2_2y, '_concat'); print()
tmpt = tab(tmpg2_2y, '_concat', 'contact_health_services_2y'); print()

# COMMAND ----------

display(tmpg2_2y)

# COMMAND ----------

tmpg1 = merge(_cohort, tmpfm, ['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'], indicator=0); print()

# Create variable contact with health services in the last 5 years
tmpg2 = tmpg1\
  .fillna(value=0, subset=[col for col in tmpfm.columns if re.match('^in_', col)])\
  .withColumn('_concat', f.concat(*[f.col(col) for col in tmpfm.columns if re.match('^in_', col)]))\
  .withColumn('contact_health_services_5y', f.when(f.col('_concat') != '00000000', 1).otherwise(0))

# check
tmpt = tab(tmpg2, 'contact_health_services_5y'); print()
tmpt = tab(tmpg2, '_concat'); print()
tmpt = tab(tmpg2, '_concat', 'contact_health_services_5y'); print()

# COMMAND ----------

display(tmpg2)

# COMMAND ----------

# MAGIC %md # 5. Create

# COMMAND ----------

# Adapt Age before merging
_cohort2 = _cohort\
  .withColumn('AGE', f.when(f.col('AGE') > 90, 90).otherwise(f.col('AGE')))


# COMMAND ----------

# MAGIC %md ## 5.1. Associate sex-age weights to each PERSON_ID

# COMMAND ----------

w1 = merge(_cohort2, tmp_w4, ['SEX','AGE'], validate='m:1', keep_results=['both', 'left_only'], indicator=0); print()

# reduce
_w1 = w1.select('PERSON_ID', 'weight_pe_mid2020', 'weight_pe_census2021')

# COMMAND ----------

display(w1)

# COMMAND ----------

display(_w1)

# COMMAND ----------

# MAGIC %md ## 5.2. Associate sex-age-region weights to each PERSON_ID

# COMMAND ----------

w2 = merge(_cohort2, tmp3_region, ['SEX','AGE','region'], validate='m:1', keep_results=['both', 'left_only'], indicator=0); print()

# reduce
_w2 = w2.select('PERSON_ID', 'weight_pe_census2021_region')

# COMMAND ----------

display(w2)

# COMMAND ----------

display(_w2)

# COMMAND ----------

# MAGIC %md ## 5.3. Associate heath services contact with PERSON_ID in cohort

# COMMAND ----------

# reduce tmpg2_2y
_w3 = merge(tmpg2.select('PERSON_ID', 'Contact_health_services_5y'),tmpg2_2y.select('PERSON_ID', 'Contact_health_services_2y'),['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'], indicator=0); print()

# COMMAND ----------

display(_w3)

# COMMAND ----------

tmpt = tab(_w3, 'Contact_health_services_2y', 'Contact_health_services_5y'); print()

# COMMAND ----------

# MAGIC %md ## 5.4. Merge all into 1 dataset

# COMMAND ----------

m1 = merge(_w1, _w2, ['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'], indicator=0); print()

# COMMAND ----------

display(m1)

# COMMAND ----------

tmp = merge(m1, _w3, ['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'], indicator=0); print()

# COMMAND ----------

# MAGIC %md # 6. Check

# COMMAND ----------

display(tmp)

# COMMAND ----------

tmpt = tabstat(tmp,'weight_pe_mid2020'); print()

# COMMAND ----------

tmpt = tabstat(tmp,'weight_pe_census2021'); print()

# COMMAND ----------

tmpt = tabstat(tmp,'weight_pe_census2021_region'); print()

# COMMAND ----------

tmpt = tab(tmp, 'Contact_health_services_2y'); print()

# COMMAND ----------

tmpt = tab(tmp, 'Contact_health_services_5y'); print()

# COMMAND ----------

# MAGIC %md # 7. Save

# COMMAND ----------

# save name
outName = f'{proj}_out_weights'.lower()

# save
tmp.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
