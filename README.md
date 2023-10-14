# Anyang_citizen  
Predicting the walk pattern of anyang citizen 

# Data prep  
```markdown
!python data_prep.py '/path/to/your/2023_09_06_00_cctvTransData.csv'
```

# TimesNet_Anomaly_detection  
Assuming that majority of the data are normal, it reconstructs original data using autoencoder --> bottleneck --> decoder (Unsupervised form).  
It uses normal MSE for the object function. 

# Detecting anomalies  
Assuming majority are normal it can be predicted that some data will not be reconstructed well, which will be selected as anomalies. Some rate of high loss will be selected as anomalies and will be labeled as anomalies.  

# Overall structure  
```markdown
23          45
25          43
23          47
21          42 
```

Where the intervals between the rows will going to be the time difference 
