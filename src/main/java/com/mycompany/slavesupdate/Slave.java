/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.slavesupdate;

import java.sql.Connection;
import java.sql.DriverManager;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.PreparedStatement;
import java.util.Map;
import org.apache.commons.io.FileUtils;
/**
 *
 * @author supramo
 */
public class Slave {

    private Connection connect = null;
    private PreparedStatement preparedStatement = null;
    public void updateSlave(){
        
        /*
        if (args.length < 2 ) {
            System.out.println(" Need three arguments");
            System.exit(1);
            
        }*/
        //args[0] = "/tmp/old";
        //args[1] = "/tmp/new";
        File oldfile = new File("/tmp/old");
        File newfile = new File("/tmp/new");
        
        List oldlist = new ArrayList();
        List newlist = new ArrayList();
        try {
            String line;
            BufferedReader reader= new BufferedReader(new FileReader(oldfile));
            while ( (line = reader.readLine()) != null ){
                oldlist.add(line);
            }
            reader.close();
            reader = new BufferedReader(new FileReader(newfile));
            while ( (line = reader.readLine()) != null ){
                newlist.add(line);
            }
            System.out.println(oldlist.size() + " " + newlist.size());
            boolean listsAreSame = true;
            if (oldlist.size() == newlist.size()){
                for (int i=0;i<oldlist.size();i++){
                    System.out.println((String)oldlist.get(i));
                    System.out.println((String) newlist.get(i));
                    if (!newlist.contains((String)oldlist.get(i))){
                        System.out.println("I came here");
                        listsAreSame = false;
                    }
                }
            }
            else
                listsAreSame = false;
            
            if (listsAreSame)
                System.exit(0);
            new Slave().sortCheckAndRemove(oldlist,newlist);
            System.out.println("Lists are not the same");
        } catch (Exception ex) {
            Logger.getLogger(Slave.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public Connection makeConnection() {
        // this will load the MySQL driver, each DB has its own driver
        try {
            Class.forName("com.mysql.jdbc.Driver");
            // setup the connection with the DB.
            return DriverManager
                    .getConnection(""
                            + "jdbc:mysql://"
                            //+ "itmd544.cbpipzbeulcc.us-west-2.rds.amazonaws.com/itmd544?"
                            //+"localhost/itmd544"
                            +"64.131.111.24/itmd544?"
                            + "user=root&password=root"
                            //+"root"
                    );
            
            
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("Could not make connection");
            System.exit(1);
        }
        
        return null;
    }

    public void sortCheckAndRemove(List oldlist, List newlist){
        //Map map = new HashMap();
        //int[] toremove = new int[Math.max(oldlist.size(),newlist.size())];
        //int[] toadd = new int[Math.max(oldlist.size(),newlist.size())];
        
        List removenode = new ArrayList();
        List addnode = new ArrayList();
        
        for (int i=0;i < newlist.size() ; i++){
            if (!oldlist.contains((String) newlist.get(i))){
                addnode.add((String) newlist.get(i));
            }
        }
        
        for (int i=0;i < oldlist.size() ; i++){
        
            if (!newlist.contains((String)oldlist.get(i))){
                removenode.add((String)oldlist.get(i));
            }
        }
        for (int k=0; k < removenode.size() ; k++){
            //System.out.println(toremove[k]);
            System.out.println("Deleting "+(String) removenode.get(k));
            removeSlave((String) removenode.get(k));
            
        }
        
        for (int k=0; k < addnode.size() ; k++){
            //System.out.println(toadd[k]);
            System.out.println("adding slave "+(String)addnode.get(k));
            try {
            Runtime r = Runtime.getRuntime();
                r.exec("/root/mountpartition "+ (String)addnode.get(k)).waitFor();
                addSlave((String)addnode.get(k));
            } catch (Exception ex) {
            System.out.println("couldnot mount");
            
            }
            
        }
        
        try {
            FileUtils.copyFile(new File("/tmp/new"), new File("/tmp/old"));
        } catch (IOException ex) {
            System.out.println("Could not copy file");
        }
    }
    
    /**
     *
     * @param s1
     */
    public void removeSlave(String s1){
        try {
            System.out.println("point 0");
            connect = makeConnection();
            System.out.println("point 1");
            preparedStatement = connect
                    .prepareStatement("delete from hadoop_slaves "
                            + " where ec2ip=?");
            preparedStatement.setString(1,s1);
            System.out.println("point 2");
            preparedStatement.executeUpdate();
            System.out.println("point 3");
            preparedStatement.close();
            connect.close();
        }
        catch(Exception e){
            System.out.println("There was a problem with updating the instance status");
        }
    }
    
    public void addSlave(String s1){
        try {
            connect = makeConnection();
            preparedStatement = connect
                    .prepareStatement("insert into hadoop_slaves "
                            + " values(?,?)");
            preparedStatement.setString(1,s1);
            preparedStatement.setString(2,"a");
            preparedStatement.executeUpdate();
            preparedStatement.close();
            connect.close();
        }
        catch(Exception e){
            System.out.println("There was a problem with inserting a record");
        }
    }
}
