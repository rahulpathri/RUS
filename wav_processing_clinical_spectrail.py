import argparse
import csv
import os
import sys
import mysql.connector
from mysql.connector import errorcode
import signal_analysis
from accumulate_logs import get_accumulated_logs
from accumulate_logs import merge_clinical_spectral

def get_wav_zoom(args):
    wav_list = []
    if os.path.isdir(args.path):
        print("Processing zoom files")
        directory=os.listdir(args.path)
        print(directory)
        try:
            cnx = mysql.connector.connect(user='root',password='D0cturnal',host='35.200.188.214',database='hasystec_timber',port='3306')
            cursor = cnx.cursor()
            wav_path = args.path
            filename = os.path.join(wav_path,"clinical_data.csv")
            if os.path.exists(filename):
                os.remove(filename)
            c = csv.writer(open(filename,'a'))
            c.writerow(["id","first_name","gender","age_years","height_feet","height_inches","weight_in_kgs","marital_status","occupation","organization","smoking","alcohol","tb_family_history","living_with_tb","paroxysmal_cough","hiv","diabetes","night_sweat","loss_of_appetite","exercise","supplement","chest_pain","current_fever_pattern","appetite_pattern","cough_frequency","cough_type","travelling_frequency","current_medications","other_cough"])
            for file in directory:
                if file.endswith(".WAV") or file.endswith(".wav"):
                    #record_id=file.split(".")[0]
                    record_id=file.split("-")[0]



                    #query2 = (" SELECT pi.record_id, pi.firstname, pi.gender, pi.age, pi.height_feet, pi.height_inches, pi.weight,pi.marital_status,pi.occupation, pi.organization,ci.smoking, ci.alcohol,ci.tb_family_history,ci.living_with_tb,ci.paroxysmal_cough,ci.hiv,ci.diabetes,ci.night_sweat,ci.loss_of_appetite,ci.exercise,ci.supplement,ci2.chestpain,ci2.current_fever_pattern,ci2.appetite_pattern,ci2.cough_frequency,ci2.cough_type,ci2.travelling_frequency,ci2.current_medication, ci2.other_cough  FROM personal_info pi INNER JOIN clinical_info_one ci ON pi.record_id=ci.record_id INNER JOIN clinical_info_two ci2 ON pi.record_id=ci2.record_id WHERE pi.record_id=%s;"%(record_id,))
                    query2 = (" SELECT pi.record_id, pi.firstname, pi.gender, pi.age, pi.height_feet, pi.height_inches, pi.weight,pi.marital_status,pi.occupation, pi.organization,ci.smoking,ci.alcohol,ci.tb_family_history,ci.living_with_tb,ci.paroxysmal_cough,ci.hiv,ci.diabetes,ci.night_sweat,ci.loss_of_appetite,ci.exercise,ci.supplement,ci2.chestpain,ci2.current_fever_pattern,ci2.appetite_pattern,ci2.cough_frequency,ci2.cough_type,ci2.travelling_frequency,ci2.current_medication, ci2.other_cough  FROM personal_info pi INNER JOIN clinical_info_one ci ON pi.record_id=ci.record_id INNER JOIN clinical_info_two ci2 ON pi.record_id=ci2.record_id WHERE pi.record_id=%s;"%(record_id,))
                    cursor.execute(query2)

                    print("record_id for query2",record_id)


                    data=cursor.fetchall()
                    print(data)
                    for row in data:
                        c.writerow(row)
            return
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)




def main(args):

        unprocessed_files = []
        if os.path.exists(args.path):
            wavfiles_list = get_wav_zoom(args)
        else:
            wavfiles_list=get_wav_android()

        unprocessed_files = os.listdir(args.path)
        print("unprocessed_files:",unprocessed_files)
        print("Number of files un-processed {}".format(len(unprocessed_files)))
        for file in unprocessed_files:
            print("file:",file)
            if file.endswith(".WAV") or file.endswith(".wav"):           
                filepath=os.path.join(args.path,file)
                print("filepath:",filepath)
                signal_analysis.extract_wav_fet(filepath, args.path, spectogrpahs=0)

        print("\n############ Accumulating training features in csv ############")
        output_path=os.path.join(args.path,args.output)
        get_accumulated_logs(output_path)
        clinical_file=os.path.join(args.path,"clinical_data.csv")
        spectral_file=os.path.join(output_path,"Accumulated_data.csv")
        goldforsas=merge_clinical_spectral(spectral_file,clinical_file)
        #path_goldforsas=os.path.join(args.path,goldforsas.csv)
        goldforsas.to_csv(args.path+'goldforsas',index=False)

if __name__ == "__main__":
    # Instantiate the parser
    parser = argparse.ArgumentParser(description='path to wavfiles')
    # Required argument
    parser.add_argument('-p', '--path', type=str, required=True,
                        help='Path of root folder containing all the wav files(Enter only if it contains zoom files)')
    parser.add_argument('-g','--spectograph',action='store_true',help='Generate spectogrpahs')
    parser.add_argument('-o','--output',type=str,default='xlxdirectory',
                       help='output directory')
    args = parser.parse_args()
    main(args)
    #wav_processor_clinical_spectral()
