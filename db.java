import java.io.File;  // Import the File class
import java.io.FileNotFoundException;  // Import this class to handle errors
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner; // Import the Scanner class to read text files
import java.util.Set;
import java.util.StringTokenizer;
import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;  // Import the IOException class to handle errors
import java.util.regex.*;
import java.io.*;
import java.util.*;

class db{


    //Index recommendation Parameters
    

    private static int SELECT_COUNT = 3;
    private static int SELECT_COUNT_THRESHOLD = 5;
    private static int NORMAL_QUERY_COLUMN_THRESHOLD = 10;
    private static int PRIORITY_QUERY_COLUMN_THRESHOLD = 5;
    static ArrayList<String> columnType = new ArrayList<>();
    static ArrayList<Integer> columnTypeLen = new ArrayList<>();
    static ArrayList<Integer> columnCard = new ArrayList<>();
    static ArrayList<ArrayList<String>> columnData = new ArrayList<ArrayList<String>>();
    public static final int file_name_padding=30;
    public static final int column_padding=20;
    public static void main(String[] args){
        try{
            if(args.length==0){
                System.out.println("No queries Filename given in System.args");
                System.exit(1);
            }
            String filename=args[0];
            File myObj = new File(filename);
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                
                switch(data.substring(0, 4)){
                    case "DROP":
                    case "drop":
                    drop_index(data);
                    break;
                    case "LIST":
                    case "list":
                    list_index(data);
                    break;
                    case "CREA":
                    case "crea":
                    create_index( data);
                    break;
                    case "SELE":
                    case "sele":
                    analyizeQuery(data);
                    break;
                    case "INSE":
                    case "inse":
                    updateInsertQueryStats(data);
                    break;
                }
            }
            myReader.close();
        }
        catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();  
        }
    }

    public static void create_index(String data){
        StringTokenizer st = new StringTokenizer(data," "); 
        ArrayList<String> index_order = new ArrayList<>();
        ArrayList<Integer> index_col_number = new ArrayList<>();
        
        // System.out.println(data);
        int index=0;
        String index_name="";
        String table_name="";
        while (st.hasMoreTokens()) { 

            String s=st.nextToken();
            index++;
            switch(index){
                case 3:
                // System.out.println(s);
                index_name=s;
                break;
                case 5:
                // System.out.println(s);
                table_name=s;
                break;
                default:
                if(index>5){
                    if (!(s.equals(",") || s.equals(")") || s.equals("("))){
                        index_order.add(s.substring(s.length()-1));
                        index_col_number.add(Integer.parseInt(s.substring(0,s.length()-1)));
                    }
                }
            }
        }

        get_table_data(table_name+".tab");

        set_index_data(index_order,index_col_number,index_name,table_name);
    }

    public static void updateInsertQueryStats(String data){
        String subParts[] = data.split(" ");
        String tableName = subParts[2];
        int runTime = Integer.parseInt(subParts[subParts.length-1]);
        try{
            File curDir = new File(".");
            File[] filesList = curDir.listFiles();

            File file3 = new File(tableName + "Analysis.txt");
            Scanner sc3 = new Scanner(file3);

            StringBuffer sb2 = new StringBuffer();
            int insertRunTime = 0;

            while(sc3.hasNextLine()){
                String nextLine = sc3.nextLine();
                if(nextLine.contains("INSERT")){
                    String insertSubParts[] = nextLine.split(" ");
                    int runTimeAnalysis = Integer.parseInt(insertSubParts[1]);
                    if(runTimeAnalysis == 0){
                        runTimeAnalysis = runTime;
                    }else{
                        runTimeAnalysis = (runTimeAnalysis + runTime)/2;
                    }
                    sb2.append("INSERT " + runTimeAnalysis+"\n");
                    insertRunTime =  runTimeAnalysis;
                }else{
                    if(sc3.hasNextLine()){
                        sb2.append(nextLine + "\n");
                    }else{
                        sb2.append(nextLine);
                    }
                }
                FileWriter fileWriter2 = new FileWriter(file3.getName());
                fileWriter2.append(sb2.toString());
                fileWriter2.flush();
                fileWriter2.close();
            }

            for(File f : filesList){
                String name = f.getName();
                if(f.isFile() && ((name.substring(name.length()-3).equals("txt") && name.contains("TempIdx"))) ){
                    String fileTableName = "" + name.charAt(0) + name.charAt(1);
                    if(fileTableName.equals(tableName)){
                        StringBuffer sb = new StringBuffer();
                        Scanner sc = new Scanner(f);
                        while(sc.hasNextLine()){
                            String nextLine = sc.nextLine();
                            if(nextLine.contains("INSERT")){
                                String subParts2[] = nextLine.split(" ");
                                int count = Integer.parseInt(subParts2[1]);
                                count++;
                                if(count >=5){
                                    File file2 = new File(tableName  + "Stats-Old.txt");
                                    Scanner sc2 = new Scanner(file2);
                                    sc2.nextLine();
                                    String insertSubParts[] = sc2.nextLine().split(" ");
                                    int oldInsertRunTime = Integer.parseInt(insertSubParts[1]);
                                    if(oldInsertRunTime*3 < insertRunTime){
                                        sb.append("INSERT " + count + " C F" );
                                    }else{
                                        sb.append("INSERT " + count + " C P" );
                                    }
                                    
                                }else{
                                    sb.append("INSERT " + count + " N F");
                                }
                            }else{
                                if(sc.hasNextLine()){
                                    sb.append(nextLine + "\n");
                                }else{
                                    sb.append(nextLine);
                                }
                            }
                        }
                        FileWriter fileWriter = new FileWriter(name);
                        fileWriter.append(sb.toString());
                        fileWriter.flush();
                        fileWriter.close();
                    }
                }
            }
        }catch(Exception e){
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void analyizeQuery(String data){
        SELECT_COUNT++;
        Set<String> predicates = getPredicatesInfo(data);

        getColumnAccessCount(data, predicates);
        
        //We can simply store the columns accessed and the runtime, so easier to compare when similar queries run. We will only compare the indexes that have full matching scan columns for ease of the project and store their runtime so if a query that comes is a priorityquery that matches any of these indexes in progress, then the count of priorityquery check can be enhanced. 

        // populateInsertQueryDetails();
        //When we do similar stuff for incoming insert query, we should also change the runtime stats of it in the analysis file of the table every time it comes in.
        String[] parts = data.split(" ");
        boolean isPriority = parts[parts.length-1].equals("P") ? true : false;
        if(isPriority){
            populatePriorityQueriesFile(data, predicates);
        }
        

        
        
        
        if(SELECT_COUNT>SELECT_COUNT_THRESHOLD){

            //This will get all the current indexes info basically table map with it's indexes in the form 1a,2d,3a  ,  1a, 2a to see if the new index should be created or not. 
            Map<String, List<String>> indexesByTable = new HashMap<>();
            getCurrentIndexesInfo(indexesByTable);

            //To check if index already exists, we will list all the existing indexes in a map with table name as key and list of indexes in the form as 1a,2b,3a and then make the sequence of the index we want to create and then see if the existing sequences already have that so don't create new index.
            //To create a temp file of index, we can write the create index ix_number on table_number (ColumnNumber Order, ColumnNumber Order)
            //Above that we will keep first parameter as PQ with it's count(if 5 is done or -5 is done) and boolean if the check is completed
            //Second parameter we will keep is boolean if the normal columns are compared and checked and completed
            //Third parameter will be of the insert if it is checked count (if 5 or -5) and boolean if the check is completed, you can even update the insert runtime with this new runtime in the analysis.txt file of the table. 
            //We will also create OldAnalysis file by copying the analysis of the table and then reset the analysis of the queries.
            Map<String, String> recommendedIndexMap = new HashMap<>();
            suggestIndex(recommendedIndexMap);
            final Map<String, String> finalIndexesToBeCreated = new HashMap<>();
            checkifTheIndexAlreadyExists(indexesByTable, recommendedIndexMap, finalIndexesToBeCreated);
            // System.out.println(finalIndexesToBeCreated.toString());
            createTempIndexIfAnalyized(finalIndexesToBeCreated, indexesByTable);

            //Index will have certain parameters to become permament, keep count of times priorityqueries have been hit and has successfully run within the previous runtime. If it crosses 5 times that parameter has been checked. 
            //Compare the column analysis of the normal columns of the table and if each column has reduced it's runtime and seek count is greater than 10 then that parameter is checked as well
            //If the insert query on this table runtime has not become bad by 3 times the previous insert time and has been checked for 5 times then this parameter has been checked. 
            //When all parameters have been checked for an index, it can be converted to a permanent index. 
            //Check calls commit for the indexes that have cleared the parameters. 
            //If insert has become bad after checking 5 times where it has slowed down by 3 times then the check can give stats and suggest user to drop the index since the health check for insert will go to -5. 
            //If the runtime of priority queries which has same predicates that our index is made on or atleast contains all the columns on which our index is made and then the runtime has not reduced , we will deduct a number, if it goes -5 then we will suggest to revoke the index
            //If the columns have been accessed 10 times for normal and priority queries and if the runtime has not improved for priority and normal then we will suggest to revoke the index for any one column of the index.
            checkCreatedIndexStats();
            commitSuccessfulIndexAnalysisResults();
            SELECT_COUNT=0;
            IndexRecommender indexRecommender = new IndexRecommender();
            indexRecommender.suggestIndex(data);
        }
        
    }

    public static Set<String> getPredicatesInfo(String data){
        System.out.println("Query to find predicates : "+ data);
        String regex = "(T|t)([0-9])+.(C|c)([0-9])+";
        Pattern pattern = Pattern.compile(regex);
        Set<String> predicates = new HashSet<>();
        Matcher matcher = pattern.matcher(data);
        while(matcher.find()){
            predicates.add(matcher.group().toUpperCase());
        }
        // predicates.forEach(p -> {
        //     System.out.println(p);
        // });
        return predicates;
    }

    public static void getColumnAccessCount(String data, Set<String> predicates){
        String[] parts = data.split(" ");
        boolean isPriority = parts[parts.length-1].equals("P") ? true : false;
        int seekTime = Integer.parseInt(parts[parts.length-2]);
        Map<String,TableInfo> map = new HashMap<>();
        predicates.forEach(predicate -> {
            String[] splits = predicate.split("\\.");
            HashSet<String> cols = new HashSet<>();
            TableInfo table = map.getOrDefault(splits[0], new TableInfo(cols, isPriority));
            table.columns.add(splits[1]);
            map.put(splits[0], table);
        });
        map.forEach((k,v) -> {
            System.out.println(k  + " and value: "+ v.columns.toString() + " and is priority: "+ v.isPriority);
            String filePath = k+"Analysis.txt";
            try{
                Scanner sc = new Scanner(new File(filePath));
                StringBuffer sb = new StringBuffer();
                Set<String> columns = v.columns;
                while(sc.hasNextLine()){
                    
                    String nextLine = sc.nextLine();
                    String subParts[] = nextLine.split(" ");
                    if(columns.contains(subParts[0])){
                        String newLine = subParts[0] + " ";
                        if(v.isPriority){
                            newLine = newLine + subParts[1] + " " + subParts[2] + " ";
                            int seekCount = Integer.parseInt(subParts[3]);
                            newLine = newLine + (seekCount+1) + " ";
                            newLine = newLine + (seekTime+ Integer.parseInt(subParts[4]))/2;
                        }else{
                            int seekCount = Integer.parseInt(subParts[1]) ;
                            newLine = newLine + (seekCount + 1) + " ";
                            newLine = newLine + (seekTime + Integer.parseInt(subParts[2]))/2 + " ";
                            newLine = newLine + subParts[3] + " " + subParts[4];
                        }
                        sb.append(newLine);
                        if(sc.hasNextLine()){
                            sb.append("\n");
                        }
                        
                    }else{
                        sb.append(nextLine);
                        if(sc.hasNextLine()){
                            sb.append( "\n");
                        }
                        
                    }
                    FileWriter fileWriter = new FileWriter(filePath);
                    fileWriter.append(sb.toString());
                    fileWriter.flush();
                    fileWriter.close();
                }
                String fileContents = sb.toString();
                
                System.out.println(fileContents);
            }catch(Exception e){
                e.printStackTrace();
                System.exit(1);
            }
            

        });
        
    }

    public static void getCurrentIndexesInfo(Map<String, List<String>> indexesByTable){
        
        File curDir = new File(".");
        File[] filesList = curDir.listFiles();
        for(File f : filesList){
            String name=f.getName();
            if(f.isFile() && (name.substring(name.length()-3).equals("idx") || (name.substring(name.length()-3).equals("txt") && name.contains("TempIdx"))) ){
                String tableName = "" + name.charAt(0) + name.charAt(1);
                // print_row(name);
                try {
                    File myObj = new File(name);
                    BufferedReader brTest = new BufferedReader(new FileReader(myObj));
                    Scanner myReader = new Scanner(myObj);
                    String st2 = brTest.readLine();
                    // for( int i=0;i<5;i++){
                    //     if (st2.hasMoreTokens()){
                    //         System.out.print(padRight(st2.nextToken(),column_padding));
                    //     }
                    //     else{
                    //         System.out.print(padRight("-",column_padding));
                    //     }
                    // }
                    // System.out.println(tableName + " " + st2);
                    List<String> indexes = indexesByTable.getOrDefault(tableName, new ArrayList<String>());
                    indexes.add(st2);
                    indexesByTable.put(tableName, indexes);

                    myReader.close();   
                    brTest.close();
                }
                catch (Exception e) {
                    System.out.println("An error occurred.");
                    e.printStackTrace();  
                }
            }

            
        }
    }

    public static void suggestIndex(Map<String, String> recommendedIndexMap){
        File curDir = new File(".");
        File[] filesList = curDir.listFiles();
        
        for(File f : filesList){
            String name=f.getName();
            if(f.isFile() && name.contains("Analysis.txt")){

                // System.out.println(name);
                String tableName = name.replaceAll("Analysis.txt", "");
                // System.out.println(tableName);
                PriorityQueue<ColumnInfo> pq = new PriorityQueue<>( (a,b) -> {
                    if(b.prioritySeekCount-a.prioritySeekCount == 0){
                        return b.normalSeekCount - a.normalSeekCount;
                    }else{
                        return b.prioritySeekCount - a.prioritySeekCount;
                    }
                });
                try{
                    File myObj = new File(name);
                    Scanner sc = new Scanner(myObj);

                    while(sc.hasNextLine()){
                        String nextLine = sc.nextLine();
                        String regex = "C([0-9])+";
                        Pattern pattern = Pattern.compile(regex);
                        Matcher matcher = pattern.matcher(nextLine);
                        if(matcher.find()){
                            // System.out.println(nextLine);
                            String subParts[] = nextLine.split(" ");
                            String columnName = subParts[0];
                            int normalSeekCount = Integer.parseInt(subParts[1]);
                            int normalRunTime = Integer.parseInt(subParts[2]);
                            int prioritySeekCount = Integer.parseInt(subParts[3]);
                            int priorityRunTime = Integer.parseInt(subParts[4]);
                            if(normalSeekCount >= NORMAL_QUERY_COLUMN_THRESHOLD && prioritySeekCount >= PRIORITY_QUERY_COLUMN_THRESHOLD){
                                ColumnInfo columnInfo = new ColumnInfo(columnName, normalSeekCount, normalRunTime, prioritySeekCount, priorityRunTime);
                                pq.add(columnInfo);
                            }
                            
                        }
                    }

                }catch(Exception e){
                    e.printStackTrace();
                    System.exit(1);
                }
                String indexSequence = "";
                while(!pq.isEmpty()){
                    // System.out.println(pq.poll().toString());
                    String columnName = pq.poll().name;
                    indexSequence += columnName.replaceAll("C","") + "A" + " ";
                }
                recommendedIndexMap.put(tableName, indexSequence);
                // System.out.println(tableName + " : "+ indexSequence);
            }
        }
    }

    public static void populatePriorityQueriesFile(String data, Set<String> predicates){
        String subParts[] = data.split(" ");
        String tableName = "";
        for(String predicate : predicates){
            String[] splits = predicate.split("\\.");
            tableName = splits[0];
        }
        // String tableName = subParts[2];
        int runTime = Integer.parseInt(subParts[subParts.length-2]);
        try{
            File curDir = new File(".");
            File[] filesList = curDir.listFiles();

            File file3 = new File(tableName + "Analysis.txt");
            Scanner sc3 = new Scanner(file3);

            StringBuffer sb2 = new StringBuffer();
            int insertRunTime = 0;

            while(sc3.hasNextLine()){
                String nextLine = sc3.nextLine();
                if(nextLine.contains("PQ")){
                    String insertSubParts[] = nextLine.split(" ");
                    int runTimeAnalysis = Integer.parseInt(insertSubParts[1]);
                    if(runTimeAnalysis == 0){
                        runTimeAnalysis = runTime;
                    }else{
                        runTimeAnalysis = (runTimeAnalysis + runTime)/2;
                    }
                    sb2.append("PQ " + runTimeAnalysis+"\n");
                    insertRunTime =  runTimeAnalysis;
                }else{
                    if(sc3.hasNextLine()){
                        sb2.append(nextLine + "\n");
                    }else{
                        sb2.append(nextLine);
                    }
                }
                FileWriter fileWriter2 = new FileWriter(file3.getName());
                fileWriter2.append(sb2.toString());
                fileWriter2.flush();
                fileWriter2.close();
            }

            for(File f : filesList){
                String name = f.getName();
                if(f.isFile() && ((name.substring(name.length()-3).equals("txt") && name.contains("TempIdx"))) ){
                    String fileTableName = "" + name.charAt(0) + name.charAt(1);
                    if(fileTableName.equals(tableName)){
                        StringBuffer sb = new StringBuffer();
                        Scanner sc = new Scanner(f);
                        while(sc.hasNextLine()){
                            String nextLine = sc.nextLine();
                            if(nextLine.contains("PQ")){
                                
                                String subParts2[] = nextLine.split(" ");
                                int count = Integer.parseInt(subParts2[1]);
                                count++;
                                if(count >=5){
                                    File file2 = new File(tableName  + "Stats-Old.txt");
                                    Scanner sc2 = new Scanner(file2);
                                    // sc2.nextLine();
                                    String insertSubParts[] = sc2.nextLine().split(" ");
                                    int oldInsertRunTime = Integer.parseInt(insertSubParts[1]);
                                    if(oldInsertRunTime < insertRunTime){
                                        
                                        sb.append("PQ " + count + " C F\n" );
                                    }else{
                                        System.out.println("Else Entered here");
                                        sb.append("PQ " + count + " C P\n" );
                                    }
                                    
                                }else{
                                    System.out.println("Entered here");
                                    sb.append("PQ " + count + " N F\n");
                                }
                            }else{
                                if(sc.hasNextLine()){
                                    sb.append(nextLine + "\n");
                                }else{
                                    sb.append(nextLine);
                                }
                            }
                        }
                        FileWriter fileWriter = new FileWriter(name);
                        fileWriter.append(sb.toString());
                        fileWriter.flush();
                        fileWriter.close();
                    }
                }
            }
        }catch(Exception e){
            e.printStackTrace();
            System.exit(1);
        }
    }


    public static void checkifTheIndexAlreadyExists(Map<String, List<String>> indexesByTable, Map<String, String> recommendedIndexMap, final Map<String, String> finalIndexesToBeCreated){
        System.out.println("Existing Indexes:");
        indexesByTable.forEach((k,v) -> {
            System.out.println(k + " existing indexes list: " + v.toString());
        });
        // System.out.println("Recommended Index: ");
        recommendedIndexMap.forEach((k,v) -> {
            if(!"".equals(v)){
                System.out.println( "Recommended index: " + v + " on table: " + k);
                List<String> existingIndexesOnTable = indexesByTable.get(k);
                
                existingIndexesOnTable.forEach(value -> {
                    if(value.contains(v)){
                        System.out.println("Recommended Index: " + v + " on table: " + k+ " is under observation or already exists so we take no action");
                    }
                });
                boolean exists = false;
                for(String existingIndex : existingIndexesOnTable){
                    if(existingIndex.contains(v)){
                        exists = true;
                    }
                }
                if(!exists){
                    Scanner sc = new Scanner(System.in);
                    System.out.println("Do you want to proceed to create a test index based on given recommendation? (Y/N)");
                    String ans =  sc.nextLine();
                    if(ans.contains("Y") || ans.contains("y")){
                        finalIndexesToBeCreated.put(k, v);
                    }
                    
                }
                
            }
            
        });
    }

    public static void checkCreatedIndexStats(){
        //Index will have certain parameters to become permament, keep count of times priorityqueries have been hit and has successfully run within the previous runtime. If it crosses 5 times that parameter has been checked. 
        //Compare the column analysis of the normal columns of the table and if each column has reduced it's runtime and seek count is greater than 10 then that parameter is checked as well
        //If the insert query on this table runtime has not become bad by 3 times the previous insert time and has been checked for 5 times then this parameter has been checked. 
        //When all parameters have been checked for an index, it can be converted to a permanent index. 
        //Check calls commit for the indexes that have cleared the parameters. 
        //If insert has become bad after checking 5 times where it has slowed down by 3 times then the check can give stats and suggest user to drop the index since the health check for insert will go to -5. 
        //If the runtime of priority queries which has same predicates that our index is made on or atleast contains all the columns on which our index is made and then the runtime has not reduced , we will deduct a number, if it goes -5 then we will suggest to revoke the index
        //If the columns have been accessed 10 times for normal and priority queries and if the runtime has not improved for priority and normal then we will suggest to revoke the index for any one column of the index.
        System.out.println("");
        File curDir = new File(".");
        File[] filesList = curDir.listFiles();
        List<File> files = new ArrayList<>();

        for(File f : filesList){
            String name=f.getName();
            if(f.isFile() && name.contains("TempIdx.txt")){
                String tableName = "" + name.charAt(0) + name.charAt(1);

                try{

                    Scanner sc5 =  new Scanner(f);
                    String columnSequence = sc5.nextLine();
                    System.out.println("Index under observation is on table: "+ tableName + " with the definition as : " + columnSequence);
                    String[] columnSeqParts = columnSequence.split("A ");
                    // System.out.println(Arrays.toString(columnSeqParts));



                    Map<String,List<Integer>> detailsByQueryInfo = new HashMap<>();
                    findDetailsOfAnalysisFile(tableName, detailsByQueryInfo);

                    Map<String,List<Integer>> detailsByQueryInfoOld = new HashMap<>();
                    findDetailsOfAnalysisFileOld(tableName, detailsByQueryInfoOld);

                    System.out.println("Analysis stats: "+ detailsByQueryInfo.toString());
                    System.out.println("Analysis stats old: "+ detailsByQueryInfoOld.toString());
                    Map<String, List<Integer>> failedColumns = new HashMap<>();

                    updateColumnStatsInTempIndex(f, detailsByQueryInfo, detailsByQueryInfoOld, columnSeqParts, failedColumns);

                    int counter = 0;
                    boolean checksFinished = true;
                    boolean indexResultsSuccessful = true;
                    List<String> failureReasons = new ArrayList<>();
                    String createIndexQuery = "";
                    Scanner sc = new Scanner(f);
                    while(sc.hasNextLine()){
                        // System.out.println(sc.nextLine());
                        String nextLine =  sc.nextLine();
                        if(counter == 1){
                            createIndexQuery = nextLine;
                        }
                        counter++;
                        if(counter > 2){
                            String subParts[] = nextLine.split(" ");
                            if(subParts[subParts.length-2].equals("N")){
                                checksFinished = false;
                            }
                            if(subParts[subParts.length-1].equals("F")){
                                indexResultsSuccessful=false;
                                String reason = subParts[0];
                                switch(reason){
                                    case "PQ":
                                        failureReasons.add("PQ");
                                        break;
                                    case "COLUMNS":
                                        failureReasons.add("COLUMNS");
                                        break;
                                    case "INSERT":
                                        failureReasons.add("INSERT");
                                        break;
                                }
                            }
                        }
                    }
                    if(checksFinished){

                        if(indexResultsSuccessful){
                            System.out.println("\nOBSERVATION:");
                            System.out.println("Priority Queries have enhanced runtime results (Checked over 5 accesses) \nColumns on which index is created have better runtime results\nNo harmful damage recognized on the inserts(Checked over 5 accesses)\nHence we can safely commit this index as permanent, Do you want to commit this index? (Y/N) \nNOTE: N will remove the temporary index permanently");
                            Scanner sc3 = new Scanner(System.in);
                            String ans = sc3.nextLine();
                            if(ans.equals("Y")){
                                //commit the index
                                create_index(createIndexQuery);
                                System.out.println("Index is successfully committed");
                                
                            }
                            if(f.delete()){
                                System.out.println("Temporary index file is deleted and index is successfully committed now");
                                list_index("LIST INDEX "+ tableName);
                            }
                            files.add(f);
                            sc3.close();
                        }else{
                            System.out.println("Index results have not shown significant enhancement due to the following reaons:");
                            int i=1;
                            for(String failureReason : failureReasons){
                                System.out.println();
                                System.out.println("REASON NO:" +i);
                                i++;
                                if(failureReason.equals("PQ")){
                                    System.out.println("Priority Queries have not shown improvement in their runtime");
                                    System.out.println("Old Priority Query Runtime:" + detailsByQueryInfoOld.get("PQ") + " New Priority Query Runtime:" + detailsByQueryInfo.get("PQ"));
                                }
                                if(failureReason.equals("COLUMNS")){
                                    System.out.println("Some columns did not improve their runtime ");
                                    failedColumns.forEach((k,v) -> {
                                        System.out.println("COLUMN"+k + "\nCurrent normal query runtime:" + v.get(0) + " priority query runtime:" + v.get(1));
                                        System.out.println("Old normal query runtime: "+ v.get(2) + " priority query runtime:" + v.get(3));
                                    });
                                }
                                if(failureReason.equals("INSERT")){
                                    System.out.println("Insert query on the table has significantly slowed down due to this index");
                                    System.out.println("Old Insert Query Runtime:" + detailsByQueryInfoOld.get("INSERT") + " New Insert Query Runtime:" + detailsByQueryInfo.get("INSERT"));
                                }
                                // System.out.println(failureReason);
                                
                            }
                            System.out.println("Would you like to drop this index, if you select no, this index will reset it's analysis and collect the data again? (Y/N)");
                            Scanner sc2  = new Scanner(System.in);
                            String ans = sc2.nextLine();
                            if(ans.equals("Y")){
                                //delete the temp file
                                if(f.delete()){
                                    System.out.println("Temporary index is successfully deleted");
                                    list_index("LIST INDEX "+ tableName);
                                }
                            }else{
                                //clear the temp file stats
                                FileWriter fileWriter = new FileWriter(name);
                                StringBuffer sb = new StringBuffer();
                                sb.append(columnSequence+"\n");
                                sb.append(createIndexQuery + "\n");
                                sb.append("PQ 0 N F\n");
                                sb.append("COLUMNS N F\n");
                                sb.append("INSERT 0 N F");
                                fileWriter.append(sb.toString());
                                fileWriter.flush();
                                fileWriter.close();
                            }
                            sc2.close();
                        }
                        
                    }
                    sc5.close();
                    
                    sc.close();
                    
                }catch(Exception e){
                    e.printStackTrace();
                    System.exit(1);
                }
                
            }
            
        }
    }

    public static void findDetailsOfAnalysisFile(String tableName, Map<String,List<Integer>> detailsByQueryInfo){
        try{
            File analysisFile = new File(tableName + "Analysis.txt");
            File myObj = new File(tableName + "Analysis.txt");
            Scanner sc4 = new Scanner(myObj);
            

            int secondCounter=0;
            while(sc4.hasNextLine()){
                String nextLine = sc4.nextLine();
                if(secondCounter==0 || secondCounter==1){
                    String[] subParts = nextLine.split(" ");
                    List<Integer> runtimes = new ArrayList<>();
                    runtimes.add(Integer.parseInt(subParts[1]));
                    detailsByQueryInfo.put(subParts[0], runtimes);
                }
                secondCounter++;
                String regex = "C([0-9])+";
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(nextLine);
                if(matcher.find()){
                    System.out.println(nextLine);
                    String subParts[] = nextLine.split(" ");
                    String columnName = subParts[0];
                    String mapKey = columnName.replaceAll("C","");
                    int normalSeekCount = Integer.parseInt(subParts[1]);
                    int normalRunTime = Integer.parseInt(subParts[2]);
                    int prioritySeekCount = Integer.parseInt(subParts[3]);
                    int priorityRunTime = Integer.parseInt(subParts[4]);
                    if(normalSeekCount >= NORMAL_QUERY_COLUMN_THRESHOLD && prioritySeekCount >= PRIORITY_QUERY_COLUMN_THRESHOLD){
                        List<Integer> runtimes = new ArrayList<>();
                        runtimes.add(normalRunTime);
                        runtimes.add(priorityRunTime);
                        
                        detailsByQueryInfo.put(mapKey, runtimes);
                    }
                    
                }
            }
            sc4.close();
        }catch(Exception e){
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void findDetailsOfAnalysisFileOld(String tableName, Map<String,List<Integer>> detailsByQueryInfoOld){
        try{
            File oldAnalysisFile = new File(tableName + "Stats-Old.txt");
            File myObjOld = new File(tableName + "Stats-Old.txt");
            Scanner sc5 = new Scanner(myObjOld);
            

            int thirdCounter=0;
            while(sc5.hasNextLine()){
                String nextLine = sc5.nextLine();
                if(thirdCounter==0 || thirdCounter ==1){
                    String[] subParts = nextLine.split(" ");
                    List<Integer> runtimes = new ArrayList<>();
                    runtimes.add(Integer.parseInt(subParts[1]));
                    detailsByQueryInfoOld.put(subParts[0], runtimes);
                }
                thirdCounter++;
                String regex = "C([0-9])+";
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(nextLine);
                if(matcher.find()){
                    // System.out.println(nextLine);
                    String subParts[] = nextLine.split(" ");
                    String columnName = subParts[0];
                    String mapKey = columnName.replaceAll("C","");
                    int normalSeekCount = Integer.parseInt(subParts[1]);
                    int normalRunTime = Integer.parseInt(subParts[2]);
                    int prioritySeekCount = Integer.parseInt(subParts[3]);
                    int priorityRunTime = Integer.parseInt(subParts[4]);
                    if(normalSeekCount >= NORMAL_QUERY_COLUMN_THRESHOLD && prioritySeekCount >= PRIORITY_QUERY_COLUMN_THRESHOLD){
                        List<Integer> runtimes = new ArrayList<>();
                        runtimes.add(normalRunTime);
                        runtimes.add(priorityRunTime);
                        // if(detailsByQueryInfo.get(mapKey)!= null){
                            
                        // }
                        detailsByQueryInfoOld.put(mapKey, runtimes);
                    }
                    
                }
            }
            sc5.close();
        }catch(Exception e){
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void updateColumnStatsInTempIndex(File f, Map<String,List<Integer>> detailsByQueryInfo, Map<String,List<Integer>> detailsByQueryInfoOld, String[] columnSeqParts,  Map<String, List<Integer>> failedColumns){
        try{
            if(detailsByQueryInfo.size() == 2){
                return;
            }
            if(detailsByQueryInfoOld.size() == 2){
                return;
            }
            for(String col : columnSeqParts){
                List<Integer> analysis = detailsByQueryInfo.get(col);
                List<Integer> oldAnalysis = detailsByQueryInfoOld.get(col);
                if(analysis != null && oldAnalysis != null && (analysis.get(0) > oldAnalysis.get(0) || analysis.get(1) > oldAnalysis.get(1))){
                    //first we add normal and pq runtime of current and then we add normal and pq runtime of old.
                    List<Integer> failedColRuntimeList = new ArrayList<>();
                    failedColRuntimeList.add(analysis.get(0));
                    failedColRuntimeList.add(analysis.get(1));
                    failedColRuntimeList.add(oldAnalysis.get(0));
                    failedColRuntimeList.add(oldAnalysis.get(1));
                    failedColumns.put(col, failedColRuntimeList);
                }
            }
            StringBuffer sb = new StringBuffer();
            Scanner sc = new Scanner(f);

            while(sc.hasNextLine()){
                String nextLine = sc.nextLine();
                if(nextLine.contains("COLUMNS")){
                    String replacement = "COLUMNS C";
                    if(failedColumns.size() == 0){
                        replacement = replacement + " P";
                    }else{
                        replacement = replacement + " F";
                    }
                    sb.append(replacement + "\n");
                }
                else{
                    if(sc.hasNextLine()){
                        sb.append(nextLine + "\n");
                    }else{
                        sb.append(nextLine );
                    }
                    
                }
            }
            FileWriter fileWriter = new FileWriter(f.getName());
            fileWriter.append(sb.toString());
            fileWriter.flush();
            fileWriter.close();
            sc.close();
        }catch(Exception e){
            e.printStackTrace();
            System.exit(1);
        }


    }

    public static void commitSuccessfulIndexAnalysisResults(){

    }

    public static void createTempIndexIfAnalyized(Map<String, String> finalIndexesToBeCreated, Map<String,List<String>> indexesByTable){
        finalIndexesToBeCreated.forEach((k,v) -> {
            if(v!= null){

                

                // CREATE INDEX X1 ON T1 ( 1A , 3A )
                String indexSyntax = "CREATE INDEX X";
                int count = indexesByTable.get(k).size()+1;
                indexSyntax += count + " ON " + k + " ( ";
                String[] sequence = v.split(" ");
                for(int i=0; i<sequence.length; i++){
                    if(i!= sequence.length-1){
                        indexSyntax = indexSyntax + sequence[i] + " , ";
                    }else{
                        indexSyntax = indexSyntax + sequence[i] + " ";
                    }
                    
                }
                for(String colNumber : sequence){
                    
                }
                indexSyntax = indexSyntax + ")";

                // System.out.println(indexSyntax);
                try{
                    String fileName = k+"X"+(count-1)+"TempIdx.txt";
                    File myObj = new File(fileName);
                    FileWriter fileWriter = new FileWriter(fileName);
                    StringBuffer sb = new StringBuffer();
                    sb.append(v+"\n");
                    sb.append(indexSyntax + "\n");
                    sb.append("PQ 0 N F\n");
                    sb.append("COLUMNS N F\n");
                    sb.append("INSERT 0 N F");
                    fileWriter.append(sb.toString());
                    fileWriter.flush();
                    fileWriter.close();
                    File analysisFile = new File(k+"Analysis.txt");
                    File oldAnalysisFile = new File(k + "Stats-Old.txt");
                    StringBuffer analysisFileSB = new StringBuffer();
                    StringBuffer oldAnalysisFileSB = new StringBuffer();
                    Scanner sc = new Scanner(analysisFile);
                    int counter=1;
                    while(sc.hasNextLine()){
                        String nextLine = sc.nextLine();
                        // System.out.println("nextLine in file: " + nextLine); 
                        if(sc.hasNextLine()){
                            oldAnalysisFileSB.append(nextLine + "\n");
                        }else{
                            oldAnalysisFileSB.append(nextLine );
                        }
                        
                        String regex = "C([0-9])+";
                        Pattern pattern = Pattern.compile(regex);
                        Matcher matcher = pattern.matcher(nextLine);
                        if(matcher.find()){
                            //C1 7 14 5 12
                            if(sc.hasNextLine()){
                                analysisFileSB.append("C"+counter + " 0 0 0 0\n" );
                            }else{
                                analysisFileSB.append("C"+counter + " 0 0 0 0" );
                            }
                            
                            counter++;
                        }else{
                            analysisFileSB.append(nextLine.split(" ")[0] + " 0\n");
                        }
                    }
                    // System.out.println("AnalysisFileSb: "+ analysisFileSB);
                    // System.out.println("OldAnalysisFileSb : "+ oldAnalysisFileSB);
                    FileWriter analysisFileWriter = new FileWriter(k+"Analysis.txt");
                    FileWriter oldAnalysisFileWriter = new FileWriter(k + "Stats-Old.txt");
                    analysisFileWriter.append(analysisFileSB.toString());
                    oldAnalysisFileWriter.append(oldAnalysisFileSB.toString());
                    analysisFileWriter.flush();
                    analysisFileWriter.close();
                    oldAnalysisFileWriter.flush();
                    oldAnalysisFileWriter.close();
                }catch(Exception e){
                    e.printStackTrace();
                    System.exit(1);
                }
                

            }
        });
    }

    public static void list_index(String data){
        int number_of_col=5;
        System.out.println("");
        StringTokenizer st = new StringTokenizer(data," ");     
        st.nextToken();
        st.nextToken();
        String table_name=st.nextToken();

        System.out.print(padRight("Index File Name",file_name_padding));
        for(int i=0; i<number_of_col;i++){
            System.out.print(padRight(Integer.toString(i+1)+"st Column", column_padding));
        }
        System.out.println("");
        
        for(int i=0;i<(number_of_col*column_padding)+file_name_padding;i++ ){
            System.out.print("_");
        }
        System.out.println("");

        File curDir = new File(".");
        File[] filesList = curDir.listFiles();
        for(File f : filesList){
            String name=f.getName();
            if(f.isFile() && name.substring(name.length()-3).equals("idx") && name.substring(0,table_name.length()).equals(table_name)){
                print_row(name);
            }
        }
        
    }

    public static void drop_index(String data){
        StringTokenizer st = new StringTokenizer(data," ");     
        st.nextToken();
        st.nextToken();
        File myObj = new File(st.nextToken()+".idx"); 
        if (myObj.delete()) { 
        System.out.println("Deleted the file: " + myObj.getName());
        } else {
        System.out.println("Failed to delete the file.");
        } 
    }

    public static void set_index_data(ArrayList<String> index_order,ArrayList<Integer> index_col_number,String index_name,String table_name){
        ArrayList<String> index_data = new ArrayList<>();
        String first_col="";
        for( int i=0;i<index_order.size();i++){
            first_col+=Integer.toString(index_col_number.get(i)) + index_order.get(i) +" ";
        }
        int index_row_num=0;
        for(ArrayList<String> row : columnData){
            index_row_num++;
            String index_row="";
            for( int i=0;i<index_col_number.size();i++){
                // System.out.println(row);
                // System.out.println(index_col_number.get(i));
                
                if(index_order.get(i).equals("A"))
                {
                    index_row+=row.get(index_col_number.get(i)-1)+" ";
                }
                else{
                    index_row+=reverse(row.get(index_col_number.get(i)-1))+" ";
                }
            }
            index_row+=";"+Integer.toString(index_row_num);
            index_data.add(index_row);
        }
        // System.out.println(index_data);

        Collections.sort(index_data,String.CASE_INSENSITIVE_ORDER);
        index_data.add(0,Integer.toString(columnData.size()));
        index_data.add(0,first_col);
        // System.out.println(index_data);
        
        
        try {
            FileWriter myWriter = new FileWriter(table_name+index_name+".idx");
            int ind=0;
            // System.out.println("File writing");
            for(String row_data: index_data){
                if(ind==0 || ind==1){
                    // System.out.println("First 2 rows");
                    myWriter.write(row_data+"\n");
                    ind++;
                    continue;
                }
                // System.out.println("Next rows");
                // System.out.println(row_data);
                StringTokenizer st = new StringTokenizer(row_data,";"); 
                String ind_data=st.nextToken();
                // System.out.println(ind_data);
                String col_num=st.nextToken();
                // System.out.println(col_num);
                String data=col_num+" '"+ind_data+"'\n";
                myWriter.write(data);
                ind++;
            }
            myWriter.close();
            // System.out.println("Successfully wrote to the file.");
          } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
          }

           
    }
    
    public static  void get_table_data(String filename){

        columnType = new ArrayList<>();
        columnTypeLen = new ArrayList<>();
        columnCard = new ArrayList<>();
        columnData = new ArrayList<ArrayList<String>>();

        int lineNumber=0;
        try 
        {
            File myObj = new File(filename);
            Scanner myReader = new Scanner(myObj);
            int number_of_Rows = 0;
            
            while (myReader.hasNextLine()) {
                lineNumber++;
                String data = myReader.nextLine();
                StringTokenizer st = new StringTokenizer(data," ");  
                
                if (lineNumber==1){
                    while (st.hasMoreTokens()) { 

                        String s=st.nextToken();
                        if (s.charAt(0)=='C'){
                            int len=0;
                            len=Integer.parseInt(s.substring(1, s.length()));
                            columnTypeLen.add(len);
                        }
                        else{
                            columnTypeLen.add(-1);
                        }
                        columnType.add(s); 
                    }
                   continue; 
                }
                else if (lineNumber==2){
                    while (st.hasMoreTokens()) {  
                        columnCard.add(Integer.parseInt(st.nextToken())); 
                    }
                   continue;
                }
                else if (lineNumber==3){
                    number_of_Rows=Integer.parseInt(data);
                    continue;
                }
                ArrayList<String> temp=new ArrayList<String>();
                int index =0;
                while (st.hasMoreTokens()) {
                    int t=columnTypeLen.get(index);
                    
                    String s=st.nextToken();
                    if ( t>0){
                        int t1=t-s.length();
                        for(int j=0;j<t1;j++){
                            s+=" ";
                        }
                    }
                    else{
                        int t1=10-s.length();
                        for(int j=0;j<t1;j++){
                            s="0"+s;
                        }
                    }
                    
                    temp.add(s);   
                    index++;
                }
                // System.out.println(temp);
                columnData.add(temp);
            }

            myReader.close();
        } 
        catch (FileNotFoundException e) 
        {
            System.out.println("An error occurred.");
            e.printStackTrace();  
        }


    }

    public static String reverse(String s)
    {
        String decoded = "";
        String alphabet = "abcdefghijklmnopqrstuvwxyz";
        String alphabet_caps = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String alphabet_caps_rev = "ZYXWVUTSRQPONMLKJIHGFEDCBA";
        
        String alphabet2 = "zyxwvutsrqponmlkjihgfedcba";
        String number1 = "0123456789";
        String number2 = "9876543210";

        for (char c : s.toCharArray()) {
            if ((c < 'a' || c > 'z') && (c < '0' || c>'9') && (c < 'A' || c > 'Z') ) {
                decoded += c; 
            } 
            else if (c >= 'a' && c <= 'z')
            {
                int pos = alphabet.indexOf(c);
                decoded += alphabet2.charAt(pos);               
            }
            else if (c >= 'A' && c <= 'Z')
            {
                int pos = alphabet_caps.indexOf(c);
                decoded += alphabet_caps_rev.charAt(pos);               
            }
            else 
            {
                // System.out.println(c);
                int pos = number1.indexOf(c);
                decoded += number2.charAt(pos);               
            }
        }
        return decoded;
    }

    public static void print_row(String name){
        System.out.print(padRight(name.substring(0,name.length()-4),file_name_padding));
        

        try {
            File myObj = new File(name);
            Scanner myReader = new Scanner(myObj);
            StringTokenizer st2 = new StringTokenizer(myReader.nextLine()," ");
            for( int i=0;i<5;i++){
                if (st2.hasMoreTokens()){
                    System.out.print(padRight(st2.nextToken(),column_padding));
                }
                else{
                    System.out.print(padRight("-",column_padding));
                }
            }
            System.out.println("");
            myReader.close();   
        }
        catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();  
        }
        

    }

    public static String padRight(String s, int n) {
        return String.format("%-" + n + "s", s);  
    }

    public static String padLeft(String s, int n) {
        return String.format("%" + n + "s", s);  
    }

    static public class TableInfo{
        HashSet<String> columns;
        boolean isPriority;

        public TableInfo(HashSet<String> columns, boolean isPriority){
            this.columns = columns;
            this.isPriority = isPriority;
        }
    }

    static public class ColumnInfo{
        String name;
        int normalSeekCount;
        int normalRunTime;
        int prioritySeekCount;
        int priorityRunTime;

        public ColumnInfo(String name, int normalSeekCount, int normalRunTime, int prioritySeekCount, int priorityRunTime){
            this.name = name;
            this.normalSeekCount = normalSeekCount;
            this.normalRunTime = normalRunTime;
            this.prioritySeekCount = prioritySeekCount;
            this.priorityRunTime = priorityRunTime;
        }

        @Override
        public String toString(){
            return (name + " " + normalSeekCount  + " " + normalRunTime + " " + prioritySeekCount + " "+ priorityRunTime); 
        }
    }
}
