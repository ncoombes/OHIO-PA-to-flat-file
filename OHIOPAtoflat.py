import pandas as pd

#user defined variables that get used at end of this python program in executing. currently executes one input file defined by file_path, with end of line char defined by
#eol_char. Exports an excel file of table to export_path location and file name.

inbound_file_path=r"S:\PHME_Files\Noel Coombes\Projects\20191220 - Parsing OHIO format PA\80855227_0_CB545BEC_TOCPA_0_MB000999_anon.dat"
eol_char="\n"
export_path=r"S:\PHME_Files\Noel Coombes\Projects\20191220 - Parsing OHIO format PA\Test_file_translated.xlsx"

#This program contains a modular series of functions (some of which depend on previous ones) to convert the OHIO w/e the fuck format into a flat excel file


#Following function takes a .dat file and converts it into a list where each element is a text string row from the input file. Strips the user defined eol_char from the string
def file_to_list_of_lines(filepath,end_of_line_char):
    with open(filepath) as rd:
        file_lines=[x.strip(end_of_line_char) for x in rd.readlines()]
    return(file_lines)
    




#This function parses a list of records (from above function eventually) and makes a list of the start and end points of a given H record and all its dependent information records
#assuming that all records following an H record relate to it till you reach another H record or reach the end of the file.
def file_H_record_spans(list_of_records):
    spans=[]
    start_index=0
    end_index=0
    for i in range(1,len(list_of_records)-1):
        if list_of_records[i][0]=="H":
            start_index=i
        if list_of_records[i+1][0]=="H" or list_of_records[i+1][0]=="T":
            end_index=i
            spans.append([start_index,end_index])
    return(spans)
    
#This function parses a given H record span from above and splits it into multiple(if needed) start and end points for I thru D thru R records.
def H_record_span_to_I_thru_R_spans(list_of_records,H_span):
    spans=[]
    start_index=0
    end_index=0
    started=False
    for i in range(H_span[0],H_span[1]+1):
        if started==False:
            if list_of_records[i][0]=="I":
                start_index=i
                started=True
              
            if list_of_records[i][0]=="D" and list_of_records[i-1][0]!="I":
                start_index=i
                started=True
               
        elif started==True:
            if list_of_records[i+1][0]=="I" or list_of_records[i+1][0]=="D" or i==(H_span[1]):
                end_index=i
                spans.append([start_index,end_index])
                started=False            
    return(spans)
    


#This function converts an I record (text string) from the OHIO format and converts it into a 'table' that can be joined/merged with the relevant other lines      
def I_record_to_table_format(txt):
    I_section=pd.DataFrame()
    I_section['I_record_type']=[txt[0]]
    diag_list=''
    i=1
    while i <len(txt):
        diag_list+=(','+txt[i:i+7].strip(' '))
        i+=7
    
    I_section['I_diagnosis_codes']=[diag_list]
    I_section['temp_key']=[1]
    return I_section
        

#Start of Mapping functions
#This function converts an D record (text string) from the OHIO format and converts it into a 'table' that can be joined/merged with the relevant other lines      
def D_record_to_table_format(txt):
    D_section=pd.DataFrame()
    D_section['D_record_type']=[txt[0]]
    D_section['D_line_item']=[txt[1:3]]
    D_section['D_authorized_eff_date']=[txt[3:11]]
    D_section['D_authorized_end_date']=[txt[11:19]]
    D_section['D_status']=[txt[19:39]]
    D_section['D_date_approved']=[txt[39:47]]
    D_section['D_service_provider_id']=[txt[47:62]]
    D_section['D_service_type_code']=[txt[62:76]]
    D_section['D_HCPCS_procedure_code']=[txt[76:82]]
    D_section['D_ndc_code']=[txt[82:93]]
    D_section['D_ICD_procedure_code']=[txt[93:100]]
    D_section['D_revenue_code']=[txt[100:106]]
    D_section['D_modifier_1']=[txt[106:108]]
    D_section['D_modifier_2']=[txt[108:110]]
    D_section['D_modifier_3']=[txt[110:112]]
    D_section['D_modifier_4']=[txt[112:114]]
    D_section['temp_key']=[1]
    return D_section

#This function converts an H record (text string) from the OHIO format and converts it into a 'table' that can be joined/merged with the relevant other lines      
def H_record_to_table_format(txt):
    H_section=pd.DataFrame()
    H_section['H_record_type']=[txt[0]]
    H_section['H_PA_assignment']=[txt[1:31]]
    H_section['H_PA_number']=[txt[31:40]]
    H_section['H_Provider_id']=[txt[40:55]]
    H_section['H_referring_id']=[txt[55:70]]
    H_section['H_current_medicaid_id']=[txt[70:78]]
    H_section['temp_key']=[1]
    return H_section

#This function converts an P record (text string) from the OHIO format and converts it into a 'table' that can be joined/merged with the relevant other lines      
def P_record_to_table_format(txt):
    P_section=pd.DataFrame()
    P_section['P_record_type']=[txt[0]]
    P_section['P_PHP_OMAP_provider_number']=[txt[1:16]]
    P_section['P_PHP']=[txt[16:66]]
    P_section['P_trading_partner_number']=[txt[66:71]]
    P_section['P_file_number']=[txt[71:78]]
    P_section['P_status']=[txt[78:128]]
    P_section['temp_key']=[1]
    return P_section
#End of mapping functions, R record mapping handled by on of the table generating functions as there is only 1 element basically to an R record, the 0-500 long text str

#This function converts an given I thru D thru R series of text records and converts them into a joined single table structure (using span functions and table converting functions)
def I_thru_R_span_to_table(list_of_records,I_to_R_span):
    I_portion=pd.DataFrame()
    D_portion=pd.DataFrame()
    R_portion=pd.DataFrame()
    R_portion['temp_key']=[1]
    R_cumalitive_text=''
    for i in range(I_to_R_span[0],I_to_R_span[1]):
        if list_of_records[i][0]=='I':
            I_portion=I_portion.append(I_record_to_table_format(list_of_records[i]))
        if list_of_records[i][0]=='D':
            D_portion=D_portion.append(D_record_to_table_format(list_of_records[i]))
        if list_of_records[i][0]=='R':
            R_cumalitive_text+=list_of_records[i][4 : len(list_of_records[i])-3 ]+' '
    R_portion['R_text']=[R_cumalitive_text]
    
    if I_portion.empty and D_portion.empty:
        I_D_R_combined=R_portion
    elif I_portion.empty:
        I_D_R_combined=pd.merge(D_portion,R_portion,how='outer',on='temp_key')
    elif D_portion.empty:
        I_D_R_combined=pd.merge(I_portion,R_portion,how='outer',on='temp_key')
    else:
        I_D_R_combined=pd.merge(I_portion,pd.merge(D_portion,R_portion,how='outer',on='temp_key'),how='outer',on='temp_key')
    return I_D_R_combined



#This function converts an given H span series of text records and converts it into a joined single table structure. If multiple I thru R spans are present for a given
#H span then the result table will have common H_sections while differing IthruR sections for the result table.
def H_thru_R_span_table(list_of_records,H_span):
    #H record is only contained on the first line of an H_span
    H_thru_R_tables=pd.DataFrame()
    H_portion=pd.DataFrame()

    if list_of_records[H_span[0]][0]=='H':
        H_portion=H_portion.append(H_record_to_table_format(list_of_records[H_span[0]]))

    
    I_thru_R_spans=H_record_span_to_I_thru_R_spans(list_of_records,H_span)
    for i in I_thru_R_spans:
        I_D_R_table=I_thru_R_span_to_table(list_of_records, i )
        H_thru_R_update_section= pd.merge(H_portion , I_D_R_table ,how='outer',on='temp_key')
        H_thru_R_tables=H_thru_R_tables.append(H_thru_R_update_section)
    
    return H_thru_R_tables

#This function converts an given P span series of text records and converts it into a joined single table structure. If multiple H spans are present for a given
#P span then the result table will have a common P_section while differing HthruR sections for the result table. Currently I'm assuming there will only be one P record per file
#its possible this is wrong and this function will need to be more generalized for a series of p records like the previous funcs.
def P_span_table(list_of_records):
    #Currently assuming that each file contains one P thru T span, thus can just submit the whole files records to this function
    P_thru_R_tables=pd.DataFrame()
    P_portion=pd.DataFrame()

    if list_of_records[0][0]=='P':
        P_portion=P_portion.append(P_record_to_table_format(list_of_records[0]))
            
    H_thru_R_spans=file_H_record_spans(list_of_records)
    for i in H_thru_R_spans:
        H_I_D_R_table=H_thru_R_span_table(list_of_records, i )
        P_thru_R_update_section= pd.merge(P_portion , H_I_D_R_table ,how='outer',on='temp_key')
        P_thru_R_tables=P_thru_R_tables.append(P_thru_R_update_section)
    
    return P_thru_R_tables

#End of defined functions
    

#actual program execution:

file_lines=file_to_list_of_lines(filepath=inbound_file_path,end_of_line_char=eol_char)
export_table=P_span_table(file_lines)
export_table.to_excel(export_path)
