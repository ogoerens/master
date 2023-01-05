package framework.Anonymization;

import experimental.Anonym;
import framework.Driver;
import org.apache.commons.configuration2.XMLConfiguration;
import org.deidentifier.arx.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

public class AnonymizationDriver {

    public void anonymize() throws Exception{
        String anonConfigFile="/home/olivier/Documents/MasterThesis/Master/src/main/resources/anonconfig.xml";
        String hierarchiesFile ="/home/olivier/Documents/MasterThesis/Master/src/main/resources/hierarchies.xml";

        XMLConfiguration anonConfig = Driver.buildXMLConfiguration(anonConfigFile);
        AnonymizationConfiguration config = new AnonymizationConfiguration(anonConfig);
        config.create();

        HierarchyManager hierarchyManager = new HierarchyManager();
        XMLConfiguration hierarchyConf = Driver.buildXMLConfiguration(hierarchiesFile);
        HierarchyStore hierarchies = hierarchyManager.buildHierarchies(hierarchyConf);

        try{
            hierarchies = hierarchyManager.buildHierarchies(hierarchyConf);
        }catch(Exception e){
            e.printStackTrace();
        }

        DataLoader dataLoader = new DataLoader();
        Data data = dataLoader.loadJDBC("jdbc:sqlserver://localhost:1433;encrypt=false;database=test;","sa",".+.QET21adg.+.","testdata");


        for (String s : hierarchies.getColumnNames()) {
            if (hierarchies.getIndexForColumnName(s)==0){
                data.getDefinition().setAttributeType(s, hierarchies.hierarchies.get(s));
            }else{
                data.getDefinition().setAttributeType(s, hierarchies.hierarchyBuilders.get(s));
            }
        }
         config.applyToData(data);

        ARXAnonymizer anonymizer = new ARXAnonymizer();
        ARXResult res = anonymizer.anonymize(data, config.getARXConfig());
        Anonym.printResult(res, data);
        System.out.println(" - Transformed data:");
        Iterator<String[]> transformed = res.getOutput(false).iterator();
        while (transformed.hasNext()) {
            System.out.print("   ");
            System.out.println(Arrays.toString(transformed.next()));
        }



    }
}
