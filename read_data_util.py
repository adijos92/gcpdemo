
class ReadDataUtil:
    #def __init__(self):

        def readCsv(self,spark,path,schema = None, inferschema = True,header = True,sep = ","):
            """
            Return new data frame by reading provided csv file
            :param self:
            :param spark: spark session
            :param path: csv file path or directory path
            :param schema: provided schema,required when inferschema is false
            :param inferschema: if true: detect file schema else false: ignore auto detect sechma
            :param header: if true: input csv file has no header
            :param sep: default: "," specify separater present in csv file
            :return:
            """
            if (inferschema is False) and (schema == None):
                raise Exception("Please provide inferschema as true or else provide schema for given input file")

            if schema == None:
                readdf = spark.read.csv(path=path,inferschema=inferschema,header=header,sep=sep)

            else:
                readdf = spark.read.csv(path=path, schema=schema, header=header, sep=sep)
                return readdf

