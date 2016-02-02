# helper class for XML to CSV conversion of NAPLAN registration records
class CSVHeaders

    @@csvheaders_students = [
        'LocalId',
        'SectorId',
        'DiocesanId',
        'OtherId',
        'TAAId',
        'StateProvinceId',
        'NationalId',
        'PlatformId',
        'PreviousLocalId',
        'PreviousSectorId',
        'PreviousDiocesanId',
        'PreviousOtherId',
        'PreviousTAAId',
        'PreviousStateProvinceId',
        'PreviousNationalId',
        'PreviousPlatformId',
        'FamilyName',
        'GivenName',
        'PreferredName',
        'MiddleName',
        'BirthDate',
        'Sex',
        'CountryOfBirth',
        'EducationSupport',
        'FFPOS',
        'VisaCode',
        'IndigenousStatus',
        'LBOTE',
        'StudentLOTE',
        'YearLevel',
        'TestLevel',
        'FTE',
        'Homegroup',
        'ClassCode',
        'ASLSchoolId',
        'SchoolLocalId',
        'LocalCampusId',
        'MainSchoolFlag',
        'OtherSchoolId',
        'ReportingSchoolId',
        'HomeSchooledStudent',
        'Sensitive',
        'OfflineDelivery',
        'Parent1SchoolEducation',
        'Parent1NonSchoolEducation',
        'Parent1Occupation',
        'Parent1LOTE',
        'Parent2SchoolEducation',
        'Parent2NonSchoolEducation',
        'Parent2Occupation',
        'Parent2LOTE',
        'AddressLine1',
        'AddressLine2',
        'Locality',
        'Postcode',
        'StateTerritory',
    ]

    @@csvheaders_staff = [
        'LocalStaffId',
        'GivenName',
        'FamilyName',
        'ClassCode',
        'HomeGroup',
        'ASLSchoolId',
        'LocalSchoolId',
        'LocalCampusId',
        'EmailAddress',
        'AdditionalInfo',
        'StaffSchoolRole',
    ]


    def CSVHeaders.get_csvheaders_students
        return @@csvheaders_students
    end

    def CSVHeaders.get_csvheaders_staff
        return @@csvheaders_staff
    end

    # Retrieve the first node matching the Xpath
    def CSVHeaders.lookup_xpath(nodes, xpath)
        @ret = nodes.at_xpath(xpath)
        return "" if @ret.nil?
        return @ret.child
    end

    # Retrieve all nodes matching the Xpath, and return them as comma-delimited string
    def CSVHeaders.lookup_xpath_multi(nodes, xpath)
        @ret = nodes.xpath(xpath)
        return "" if @ret.nil?
        return @ret.map { |x| x.child.to_s }.join(",")
        # we don't want the native ruby csv encoding of nested arrays, which is "[""x", ""y""]", but just "x,y"
    end

    # Convert a CSV object to a CSV array, relying on the given CSV headers 
    def CSVHeaders.csv_object2array(csv, csvheaders)
        @ret = Array.new(csvheaders.length)
        csvheaders.each_with_index do |key, i|
            @ret[i] = csv[key]
        end
        return @ret
    end

    @@naplan_student_csv_jsonschema =   
{
  "type": "object",
  "properties": {
    "LocalId": { "type": "string" },
    "SectorId": { "type": "string" },
    "DiocesanId": { "type": "string" },
    "OtherId": { "type": "string" },
    "TAAId": { "type": "string" },
    "StateProvinceId": { "type": "string" },
    "NationalId": { "type": "string" },
    "PlatformId": { "type": "string" },
    "PreviousLocalId": { "type": "string" },
    "PreviousSectorId": { "type": "string" },
    "PreviousDiocesanId": { "type": "string" },
    "PreviousOtherId": { "type": "string" },
    "PreviousTAAId": { "type": "string" },
    "PreviousStateProvinceId": { "type": "string" },
    "PreviousNationalId": { "type": "string" },
    "PreviousPlatformId": { "type": "string" },
    "FamilyName": { "type": "string" },
    "GivenName": { "type": "string" },
    "PreferredName": { "type": "string" },
    "MiddleName": { "type": "string" },
    "BirthDate": { "type": "string" },
    "Sex": { "type": "integer", "enum": [1, 2, 3, 9] },
    "CountryOfBirth": { "type": "string" },
    "EducationSupport": { "type": "string" },
    "FFPOS": { "type": "string" },
    "VisaCode": { "type": "string" },
    "IndigenousStatus": { "type": "string" },
    "LBOTE": { "type": "string" },
    "StudentLOTE": { "type": "string" },
    "YearLevel": { "type": "string" },
    "TestLevel": { "type": "string" },
    "FTE": { "type": "string" },
    "Homegroup": { "type": "string" },
    "ClassCode": { "type": "string" },
    "ASLSchoolId": { "type": "string" },
    "SchoolLocalId": { "type": "string" },
    "LocalCampusId": { "type": "string" },
    "MainSchoolFlag": { "type": "string" },
    "OtherSchoolId": { "type": "string" },
    "ReportingSchoolId": { "type": "string" },
    "HomeSchooledStudent": { "type": "string" },
    "Sensitive": { "type": "string" },
    "OfflineDelivery": { "type": "string" },
    "Parent1SchoolEducation": { "type": "string" },
    "Parent1NonSchoolEducation": { "type": "string" },
    "Parent1Occupation": { "type": "string" },
    "Parent1LOTE": { "type": "string" },
    "Parent2SchoolEducation": { "type": "string" },
    "Parent2NonSchoolEducation": { "type": "string" },
    "Parent2Occupation": { "type": "string" },
    "Parent2LOTE": { "type": "string" },
    "AddressLine1": { "type": "string" },
    "AddressLine2": { "type": "string" },
    "Locality": { "type": "string" },
    "Postcode": { "type": "string" },
    "StateTerritory": { "type": "string" }
  }
}



    @@naplan_staff_csv_jsonschema = 
{
  "type": "object",
  "properties": {
    "LocalStaffId": { "type": "string" },
    "FamilyName": { "type": "string" },
    "GivenName": { "type": "string" },
    "HomeGroup": { "type": "string" },
    "ClassCode": { "type": "string" },
    "ASLSchoolId": { "type": "string" },
    "LocalSchoolId": { "type": "string" },
    "LocalCampusId": { "type": "string" },
    "EmailAddress": { "type": "string" },
    "AdditionalInfo": { "type": "string" ,  "enum": ["Y", "N"]},
    "StaffSchoolRole": { "type": "string" },
  }
}


# http://w3c.github.io/csvw/syntax/

    @@naplan_student_csv_csvw = <<JSON
{
  "@context": "http://www.w3.org/ns/csvw",
  "null": true,
  "tables": [{
  "url": "naplan_student_csv_csvw.csv",
  "tableSchema": {
     "columns": [ 
      {"name": "LocalId", "datatype": {"base": "string"}},
      {"name": "SectorId", "datatype": {"base": "string"}},
      {"name": "DiocesanId", "datatype": {"base": "string"}},
      {"name": "OtherId", "datatype": {"base": "string"}},
      {"name": "TAAId", "datatype": {"base": "string"}},
      {"name": "StateProvinceId", "datatype": {"base": "string"}},
      {"name": "NationalId", "datatype": {"base": "string"}},
      {"name": "PlatformId", "datatype": {"base": "string"}},
      {"name": "PreviousLocalId", "datatype": {"base": "string"}},
      {"name": "PreviousSectorId", "datatype": {"base": "string"}},
      {"name": "PreviousDiocesanId", "datatype": {"base": "string"}},
      {"name": "PreviousOtherId", "datatype": {"base": "string"}},
      {"name": "PreviousTAAId", "datatype": {"base": "string"}},
      {"name": "PreviousStateProvinceId", "datatype": {"base": "string"}},
      {"name": "PreviousNationalId", "datatype": {"base": "string"}},
      {"name": "PreviousPlatformId", "datatype": {"base": "string"}},
      {"name": "FamilyName", "datatype": {"base": "string"}},
      {"name": "GivenName", "datatype": {"base": "string"}},
      {"name": "PreferredName", "datatype": {"base": "string"}},
      {"name": "MiddleName", "datatype": {"base": "string"}},
      {"name": "BirthDate", "datatype": {"base": "date", "format": "yyyy-MM-dd"}},
      {"name": "Sex", "datatype": {"base": "string", "format": "^[1239]$" }},
      {"name": "CountryOfBirth", "datatype": {"base": "string", "format": "^[0-9][0-9][0-9][0-9]$"}},
      {"name": "EducationSupport", "datatype": {"base": "string", "format": "^[NUYX]$"}},
      {"name": "FFPOS", "datatype": {"base": "string", "format": "^[129]$"}},
      {"name": "VisaCode", "datatype": {"base": "string", "format": "^[0-9][0-9][0-9]$"}},
      {"name": "IndigenousStatus", "datatype": {"base": "string", "format": "^[12349]$"}},
      {"name": "LBOTE", "datatype": {"base": "string", "format": "^[NUXY]$"}},
      {"name": "StudentLOTE", "datatype": {"base": "string", "format": "^[0-9][0-9][0-9][0-9]$" }},
      {"name": "YearLevel", "datatype": {"base": "string", "format": "^([1-9]|1[12]|11MINUS|12PLUS|CC|K|K3|K4|P|PS|UG|UGJunSec|UGPri|UGSec|UGSnrSec)$"}},
      {"name": "TestLevel", "datatype": {"base": "string", "format": "^[3579]$"}},
      {"name": "FTE", "datatype": {"base": "decimal", "minimum": 0, "maximum": 1}},
      {"name": "Homegroup", "datatype": {"base": "string"}},
      {"name": "ClassCode", "datatype": {"base": "string"}},
      {"name": "ASLSchoolId", "datatype": {"base": "string"}},
      {"name": "SchoolLocalId", "datatype": {"base": "string"}},
      {"name": "LocalCampusId", "datatype": {"base": "string"}},
      {"name": "MainSchoolFlag", "datatype": {"base": "string", "format": "^0[123]$"}},
      {"name": "OtherSchoolId", "datatype": {"base": "string"}},
      {"name": "ReportingSchoolId", "datatype": {"base": "string"}},
      {"name": "HomeSchooledStudent", "datatype": {"base": "string", "format": "^[YUXN]$"}},
      {"name": "Sensitive", "datatype": {"base": "string", "format": "^[YUXN]$"}},
      {"name": "OfflineDelivery", "datatype": {"base": "string", "format": "^[YUXN]$"}},
      {"name": "Parent1SchoolEducation", "datatype": {"base": "string", "format": "^[01234]$"}},
      {"name": "Parent1NonSchoolEducation", "datatype": {"base": "string", "format": "^[05678]$"}},
      {"name": "Parent1Occupation", "datatype": {"base": "string", "format": "^[123489]$"}},
      {"name": "Parent1LOTE", "datatype": {"base": "string", "format": "^[0-9][0-9][0-9][0-9]$"}},
      {"name": "Parent2SchoolEducation", "datatype": {"base": "string", "format": "^[01234]$"}},
      {"name": "Parent2NonSchoolEducation", "datatype": {"base": "string", "format": "^[05678]$"}},
      {"name": "Parent2Occupation", "datatype": {"base": "string", "format": "^[123489]$"}},
      {"name": "Parent2LOTE", "datatype": {"base": "string", "format": "^[0-9][0-9][0-9][0-9]$"}},
      {"name": "AddressLine1", "datatype": {"base": "string"}},
      {"name": "AddressLine2", "datatype": {"base": "string"}},
      {"name": "Locality", "datatype": {"base": "string"}},
      {"name": "Postcode", "datatype": {"base": "string", "format": "^[0-9][0-9][0-9][0-9]$"}},
      {"name": "StateTerritory", "datatype": {"base": "string", "format": "^(NSW|VIC|TAS|SA|WA|NT|QLD|ACT)$"}}
    ]}}]
}
JSON

    @@naplan_staff_csv_csvw = <<JSON 
{
  "@context": "http://www.w3.org/ns/csvw",
  "null": true,
  "tables": [{
  "url": "naplan_student_csv_csvw.csv",
  "tableSchema": {
     "columns": [ 
      {"name": "LocalId", "datatype": {"base": "string"}},
      {"name": "FamilyName", "datatype": {"base": "string"}},
      {"name": "GivenName", "datatype": {"base": "string"}},
      {"name": "Homegroup", "datatype": {"base": "string"}},
      {"name": "ClassCode", "datatype": {"base": "string"}},
      {"name": "ASLSchoolId", "datatype": {"base": "string"}},
      {"name": "SchoolLocalId", "datatype": {"base": "string"}},
      {"name": "LocalCampusId", "datatype": {"base": "string"}},
      /* http://www.regular-expressions.info/email.html */
      {"name": "EmailAddress", "datatype": {"base": "string", "format": "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-zA-Z][a-zA-Z]+" }},
      {"name": "ReceiveAdditionalInformation", "datatype": {"base": "boolean", "format": "Y|N"}},
      {"name": "StaffSchoolRole", "datatype": {"base": "string"}}
    ]}}]
}
JSON

    def CSVHeaders.get_naplan_staff_csv_jsonschema
        return @@naplan_staff_csv_jsonschema
    end
    def CSVHeaders.get_naplan_student_csv_jsonschema
        return @@naplan_student_csv_jsonschema
    end
    def CSVHeaders.get_naplan_staff_csv_csvw
        return @@naplan_staff_csv_csvw
    end
    def CSVHeaders.get_naplan_student_csv_csvw
        return @@naplan_student_csv_csvw
    end


end
