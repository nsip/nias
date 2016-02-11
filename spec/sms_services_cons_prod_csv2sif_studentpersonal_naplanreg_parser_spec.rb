
require "net/http"
require "spec_helper"
require 'poseidon' 

csv = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

long_localid = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371fjghh371fjghh371fjghh371fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

blank_param = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
 ,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_stateterritory = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,Queensland
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_postcode = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,800,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_parent2occupation = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,7,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_parent1occupation = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,machinist,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_parent2nonschooleducation = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,1,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_parent1nonschooleducation = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,05,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_parent2schooleducation = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,5,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_parent1schooleducation = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,01,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_mainschoolflag = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,1,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_fte = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,1.2,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_testlevel = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,8,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_studentlote = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,12,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_parent1lote = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,12010,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_parent2lote = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,English,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_yearlevel = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,9a,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_indigenousstatus = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,12,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_visacode = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,1010,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

# FFPOS = 6
invalid_csv_ffpos = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,6,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_lbote = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,No way!,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_homeschooledstudent = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,No way!,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_sensitive = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,No way!,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_offlinedelivery = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,No way!,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

invalid_csv_educationsupport = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,No way!,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

# CountryOfBirth has invalid format: 1101
invalid_csv_countryofbirth = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,110,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

# Sex has value 6: invalid against NAPLAN schema
invalid_csv_sex = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,6,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

# Birth date has wrong format: invalid against NAPLAN schema
invalid_csv_birthdate = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,26072004,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

# 7 years old, goes to year 9
inconsistent_csv_birthdate = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,9,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

# UG year level
ug_year_level = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,UG,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

# Year level 7, test level 9
mismatch_year_test_level = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,9,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

# Year level 8, test level 9
wrong_year_level = <<CSV
LocalId,SectorId,DiocesanId,OtherId,TAAId,StateProvinceId,NationalId,PlatformId,PreviousLocalId,PreviousSectorId,PreviousDiocesanId,PreviousOtherId,PreviousTAAId,PreviousStateProvinceId,PreviousNationalId,PreviousPlatformId,FamilyName,GivenName,PreferredName,MiddleName,BirthDate,Sex,CountryOfBirth,EducationSupport,FFPOS,VisaCode,IndigenousStatus,LBOTE,StudentLOTE,YearLevel,TestLevel,FTE,Homegroup,ClassCode,ASLSchoolId,SchoolLocalId,LocalCampusId,MainSchoolFlag,OtherSchoolId,ReportingSchoolId,HomeSchooledStudent,Sensitive,OfflineDelivery,Parent1SchoolEducation,Parent1NonSchoolEducation,Parent1Occupation,Parent1LOTE,Parent2SchoolEducation,Parent2NonSchoolEducation,Parent2Occupation,Parent2LOTE,AddressLine1,AddressLine2,Locality,Postcode,StateTerritory
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,7,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
fjghh371,14668,65616,75189,50668,59286,35164,47618,66065,4716,50001,65241,55578,44128,37734,73143,Seefeldt,Treva,Treva,E,2004-07-26,2,1101,Y,1,101,2,Y,2201,8,7,0.89,7E,7D,k460,046129,01,02,k460,k460,U,Y,Y,3,8,2,1201,2,7,4,1201,30769 PineTree Rd.,,Pepper Pike,9999,QLD
CSV

wrong_record = <<CSV
LocalStaffId,GivenName,FamilyName,ClassCode,HomeGroup,ASLSchoolId,LocalSchoolId,LocalCampusId,EmailAddress,AdditionalInfo,StaffSchoolRole
fjghh371,Treva,Seefeldt,7D,7E,k460,046129,01,tseefeldt@example.com,Y,teacher
fjghh371,Treva,Seefeldt,7D,7E,k460,046129,01,tseefeldt@example.com,Y,teacher
CSV


out = <<XML
<StudentPersonal xmlns="http://www.sifassociation.org/au/datamodel/3.4" RefId="A5413EDF-886B-4DD5-A765-237BEDEC9833">
  <LocalId>fjghh371</LocalId>
  <StateProvinceId>59286</StateProvinceId>
  <OtherIdList>
    <OtherId Type="SectorStudentId">14668</OtherId>
    <OtherId Type="DiocesanStudentId">65616</OtherId>
    <OtherId Type="OtherStudentId">75189</OtherId>
    <OtherId Type="TAAStudentId">50668</OtherId>
    <OtherId Type="NationalStudentId">35164</OtherId>
    <OtherId Type="NAPPlatformStudentId">47618</OtherId>
    <OtherId Type="PreviousLocalSchoolStudentId">66065</OtherId>
    <OtherId Type="PreviousSectorStudentId">4716</OtherId>
    <OtherId Type="PreviousDiocesanStudentId">50001</OtherId>
    <OtherId Type="PreviousOtherStudentId">65241</OtherId>
    <OtherId Type="PreviousTAAStudentId">55578</OtherId>
    <OtherId Type="PreviousStateProvinceId">44128</OtherId>
    <OtherId Type="PreviousNationalStudentId">37734</OtherId>
    <OtherId Type="PreviousNAPPlatformStudentId">73143</OtherId>
  </OtherIdList>
  <PersonInfo>
    <Name Type="LGL">
      <FamilyName>Seefeldt</FamilyName>
      <GivenName>Treva</GivenName>
      <MiddleName>E</MiddleName>
      <PreferredGivenName>Treva</PreferredGivenName>
    </Name>
    <Demographics>
      <IndigenousStatus>2</IndigenousStatus>
      <Sex>2</Sex>
      <BirthDate>2004-07-26</BirthDate>
      <CountryOfBirth>1101</CountryOfBirth>
      <LanguageList>
        <Language>
          <Code>2201</Code>
          <LanguageType>4</LanguageType>
        </Language>
      </LanguageList>
      <VisaSubClass>101</VisaSubClass>
      <LBOTE>Y</LBOTE>
    </Demographics>
    <AddressList>
      <Address Type="0765" Role="012B">
        <Street>
          <Line1>30769 PineTree Rd.</Line1>
        </Street>
        <City>Pepper Pike</City>
        <StateProvince>QLD</StateProvince>
        <Country>1101</Country>
        <PostalCode>9999</PostalCode>
      </Address>
    </AddressList>
  </PersonInfo>
  <MostRecent>
    <SchoolLocalId>046129</SchoolLocalId>
    <YearLevel>
      <Code>7</Code>
    </YearLevel>
    <FTE>0.89</FTE>
    <Parent1Language>1201</Parent1Language>
    <Parent2Language>1201</Parent2Language>
    <Parent1EmploymentType>2</Parent1EmploymentType>
    <Parent2EmploymentType>4</Parent2EmploymentType>
    <Parent1SchoolEducationLevel>3</Parent1SchoolEducationLevel>
    <Parent2SchoolEducationLevel>2</Parent2SchoolEducationLevel>
    <Parent1NonSchoolEducation>8</Parent1NonSchoolEducation>
    <Parent2NonSchoolEducation>7</Parent2NonSchoolEducation>
    <LocalCampusId>01</LocalCampusId>
    <SchoolACARAId>k460</SchoolACARAId>
    <TestLevel>
      <Code>7</Code>
    </TestLevel>
    <Homegroup>7E</Homegroup>
    <ClassCode>7D</ClassCode>
    <MembershipType>02</MembershipType>
    <FFPOS>1</FFPOS>
    <ReportingSchoolId>k460</ReportingSchoolId>
    <OtherEnrollmentSchoolACARAId>k460</OtherEnrollmentSchoolACARAId>
  </MostRecent>
  <EducationSupport>Y</EducationSupport>
  <HomeSchooledStudent>U</HomeSchooledStudent>
  <Sensitive>Y</Sensitive>
  <OfflineDelivery>Y</OfflineDelivery>
</StudentPersonal>
XML
out.gsub!(/\n[ ]*/,"").chomp!

@service_name = 'sms_services_cons_prod_csv2sif_studentpersonal_naplanreg_parser_spec'

describe "NAPLAN convert CSV to SIF" do

    def post_csv(csv) 
        request = Net::HTTP::Post.new("/naplan/csv")
        request.body = csv
        request["Content-Type"] = "text/csv"
        @http.request(request)
    end

    before(:all) do
        @http = Net::HTTP.new("localhost", "9292")
        @xmlconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "naplan.sifxmlout.none", 0, :latest_offset)
        puts "Next offset    = #{@xmlconsumer.next_offset}"
        sleep 1
    end

    context "Valid CSV to naplan.csv" do
        before(:example) do
        	post_csv(csv)
        end
        it "pushes templated XML to naplan.sifxmlout.none" do
            sleep 5
            begin
                a = @xmlconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
                a[0].value.gsub!(%r{<StudentPersonal xmlns="http://www.sifassociation.org/au/datamodel/3.4" RefId="[^"]+">}, '<StudentPersonal xmlns="http://www.sifassociation.org/au/datamodel/3.4" RefId="A5413EDF-886B-4DD5-A765-237BEDEC9833">').gsub!(%r{<\?xml version="1.0"\?>},'').gsub!(/<!-- CSV [^>]+>/, "").gsub!(/\n[ ]*/,"")
                expect(a[0].value).to eq out
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: sex enumerable" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_sex)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'6' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: FFPOS enumerable" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_ffpos)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'6' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: birthdate format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_birthdate)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'26072004' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: birthdate inconsistent with year level" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(inconsistent_csv_birthdate)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["inconsistent"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: country of birth format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_countryofbirth)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'110' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: education support format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_educationsupport)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
                a.each { |e| puts e}
				errors = a.find_all{ |e| e.value["'No way!' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: LBOTE format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_lbote)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'No way!' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: HomeSchooledStudent format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_homeschooledstudent)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'No way!' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: Sensitive format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_sensitive)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'No way!' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: OfflineDelivery format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_offlinedelivery)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'No way!' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

=begin
    context "Invalid CSV to naplan.csv: VisaCode format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_visacode)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'1010' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end
=end

    context "Invalid CSV to naplan.csv: IndigenousStatus format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_indigenousstatus)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'12' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: StudentLOTE format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_studentlote)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'12' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: Parent1LOTE format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_parent1lote)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'12010' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: Parent2LOTE format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_parent2lote)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'English' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: YearLevel format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_yearlevel)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'9a' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: TestLevel format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_testlevel)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'8' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: FTE format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_fte)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'1.2' is greater than the maximum value allowed"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: MainSchoolFlag format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_mainschoolflag)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'1' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: Parent1SchoolEducation format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_parent1schooleducation)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'01' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: Parent2SchoolEducation format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_parent2schooleducation)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'5' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: Parent1NonSchoolEducation format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_parent1nonschooleducation)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'05' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: Parent2NonSchoolEducation format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_parent2nonschooleducation)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'1' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end
    
    context "Invalid CSV to naplan.csv: Parent1Occupation format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_parent1occupation)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'machinist' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: Parent2Occupation format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_parent2occupation)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'7' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: Postcode format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_postcode)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'800' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

    context "Invalid CSV to naplan.csv: StateTerritory format" do
        before(:example) do
        	@errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
        	puts "Next offset    = #{@errorconsumer.next_offset}"
        	post_csv(invalid_csv_stateterritory)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
				errors = a.find_all{ |e| e.value["'Queensland' is not a valid value"] }
				expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
        after(:example) do
        end
    end

   context "Blank mandatory parameter in CSV to naplan.csv" do
        before(:example) do
                @errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
                puts "Next offset    = #{@errorconsumer.next_offset}"
                post_csv(blank_param)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
puts a.inspect
                                errors = a.find_all{ |e| e.value["element is not expected"] }
                                expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
    end



   context "Staff record in CSV to naplan.csv" do
        before(:example) do
                @errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
                puts "Next offset    = #{@errorconsumer.next_offset}"
                post_csv(wrong_record)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
                                errors = a.find_all{ |e| e.value["You appear to have submitted a"] }
                                expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
    end

   context "Long LocalId in CSV to naplan.csv" do
        before(:example) do
                @errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
                puts "Next offset    = #{@errorconsumer.next_offset}"
                post_csv(long_localid)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
                                errors = a.find_all{ |e| e.value["is too long"] }
                                expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
    end

   context "UG Year Level in CSV to naplan.csv" do
        before(:example) do
                @errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
                puts "Next offset    = #{@errorconsumer.next_offset}"
                post_csv(ug_year_level)
        end
        it "pushes warning to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
a.each {|e| puts e.value }
                                errors = a.find_all{ |e| e.value["Warning"] }
                                expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
    end

  context "Mismatch between Year Level and Test Level in CSV to naplan.csv" do
        before(:example) do
                @errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
                puts "Next offset    = #{@errorconsumer.next_offset}"
                post_csv(mismatch_year_test_level)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
                                errors = a.find_all{ |e| e.value["does not match Test Level"] }
                                expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
    end

  context "Bad Year Level and Test Level in CSV to naplan.csv" do
        before(:example) do
                @errorconsumer = Poseidon::PartitionConsumer.new(@service_name, "localhost", 9092, "csv.errors", 0, :latest_offset)
                puts "Next offset    = #{@errorconsumer.next_offset}"
                post_csv(wrong_year_level)
        end
        it "pushes error to csv.errors" do
            sleep 5
            begin
                a = @errorconsumer.fetch
                expect(a).to_not be_nil
                expect(a.empty?).to be false
                                errors = a.find_all{ |e| e.value["not appropriate for NAPLAN"] }
                                expect(errors.empty?).to be false
            rescue Poseidon::Errors::OffsetOutOfRange
                puts "[warning] - bad offset supplied, resetting..."
                offset = :latest_offset
                retry
            end
        end
    end



    after(:all) do
        #sleep 5
    end

end
