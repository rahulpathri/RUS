#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os,inspect
import seaborn as sns
from sklearn.metrics import accuracy_score, f1_score, confusion_matrix,roc_curve,roc_auc_score,classification_report,auc
from sklearn import metrics
from sklearn.model_selection import KFold, StratifiedKFold
from datetime import datetime
from sklearn.model_selection import train_test_split
import pickle
from imblearn.ensemble import RUSBoostClassifier
from sklearn.tree import DecisionTreeClassifier
import warnings
warnings.filterwarnings('ignore')
import random
random.seed(100)
#=H2/(G2)^2*10000 formula for BMI
 


# In[2]:
 


get_dir = os.getcwd()
get_dir


# In[3]:

train=pd.read_excel(get_dir+"\\Train\\warmup_oldTB_1.xlsx")

test=pd.read_excel(get_dir+"\\Score\\scoreingA.xlsx")
 



# In[4]:


train


# In[5]:


miss=train.isnull().sum()
miss=miss[miss>0]
miss=pd.DataFrame(miss, columns=['Count'])
miss['Name']=miss.index
miss_cols=miss['Name'].tolist()
sns.barplot(x='Count',y='Name',data=miss)


# In[6]:


train = train.dropna()


# In[7]:


train


# In[8]:


from sklearn.preprocessing import LabelEncoder
le=LabelEncoder()
train['Gender']=le.fit_transform(train['Gender'])


# In[9]:


le_test=LabelEncoder()
test['Gender']=le_test.fit_transform(test['Gender'])


# In[10]:


train


# In[11]:


X=train.iloc[:,1:287].values
#X=train.iloc[:,1:127].values

y=train['Target_status'].values


# In[12]:


X_train,X_test,y_train,y_test=train_test_split(X,y,random_state=42,test_size=0.05)
clf_tree= DecisionTreeClassifier(min_samples_split=2,min_samples_leaf=42)


# In[ ]:





# In[13]:


rusboost = RUSBoostClassifier(estimator=clf_tree,  n_estimators=16, learning_rate=0.09, random_state=42,
       replacement=False, sampling_strategy='auto')
rusboost.fit(X_train,y_train)  
predictions_val_CT = rusboost.predict(X_test)
predictions_train_CT = rusboost.predict(X_train)
predictions_val_CT1 = rusboost.predict_proba(X_test)[:,1:]


# In[14]:


print (classification_report(y_true=y_test,y_pred=predictions_val_CT))
print ("AUC score{:2.2}".format(roc_auc_score(y_test, predictions_val_CT)))
print(accuracy_score(y_test,predictions_val_CT))
print(accuracy_score(y_train,predictions_train_CT))


# In[15]:


#from sklearn.model_selection import cross_val_score
#scores = np.average(cross_val_score(rusboost, X_train, y_train, cv=10))


# In[ ]:


#scores


# In[16]:


TN,FP,FN,TP =  confusion_matrix(y_pred=predictions_val_CT,y_true=y_test).ravel()
senstivity = TP/float(TP+FP)
specificity = TN/float(TN+FP)
print("TP", TP)
print("TN", TN)
print("FP", FP)
print("FN", FN)
print ("Recall= {}".format(TP / float(TP + FN)))
print ("Precison= {}".format(TP / float(TP + FP)))
print ("specificity= {}".format(TN / float(TN + FP)))
print ("positive Likely hood (precision/1-specificty) = {}".format(senstivity/float(1-specificity)))


# In[18]:


pred_df=pd.DataFrame(predictions_val_CT,columns=['Pred'])
pred_df['actual']=y_test



# In[17]:


filename = 'Covid_model.sav'
pickle.dump(rusboost, open(get_dir+"\\Covid_model1.sav", 'wb'))



# In[19]:


pred_df


# In[20]:


predictions_val_CT


# In[21]:


predictions_val_CT1


# In[22]:


test


# In[23]:


predictions_test_CT = rusboost.predict(test)
predictions_test_CT1 = rusboost.predict_proba(test)[:,1:]
print(predictions_test_CT1)
print(predictions_test_CT)


# In[24]:


predictions_test_CT


# In[25]:


predictions_test_CT1


# In[ ]:




