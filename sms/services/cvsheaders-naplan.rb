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
        'LocalId',
        'GivenName',
        'FamilyName',
        'Homegroup',
        'ClassCode',
        'ASLSchoolId',
        'SchoolLocalId',
        'LocalCampusId',
        'EmailAddress',
        'ReceiveAdditionalInformation',
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

end
