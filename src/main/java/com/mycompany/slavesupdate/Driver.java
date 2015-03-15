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
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
/**
 *
 * @author supramo
 */
public class Driver {

    private Connection connect = null;
    private PreparedStatement preparedStatement = null;
    public static void main(String[] args){
        
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
            System.out.println((String)oldlist.get(0) +" " + (String)newlist.get(0));
            new Driver().sortCheckAndRemove(oldlist,newlist);
            System.out.println("Lists are not the same");
        } catch (Exception ex) {
            Logger.getLogger(Driver.class.getName()).log(Level.SEVERE, null, ex);
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
                            + "64.131.111.18/itmd544?"
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
        Map map = new HashMap();
        int[] a = new int[oldlist.size()];
        int[] b = new int[newlist.size()];
        for (int i=0;i < oldlist.size() ; i++){
            System.out.println("String to be split " + (String)oldlist.get(i));
            String[] stringarr = ((String)oldlist.get(i)).split("\\.");
            System.out.println("The size is " + stringarr.length + " " + ((String)oldlist.get(i)).split(".").toString());
            String str = "";
            for (int j=0;j<stringarr.length;j++){
                str += stringarr[j];
            }
            System.out.println("The string is "+ str);
            a[i] = Integer.parseInt(str);
            map.put(str,(String)oldlist.get(i));
        }
        
        for (int i=0;i < newlist.size() ; i++){
            String[] stringarr = ((String)newlist.get(i)).split("\\.");
            String str = "";
            for (int j=0;j<stringarr.length;j++){
                str += stringarr[j];
            }
            b[i] = Integer.parseInt(str);
            map.put(str,(String)newlist.get(i));
        }
        Arrays.sort(a);Arrays.sort(b);    
        int i = 0 ,j = 0;
        int[] toremove = new int[Math.max(a.length, b.length)];
        int[] toadd = new int[Math.max(a.length, b.length)];
        int toa=0;int tor = 0;
        System.out.println(a.length + "  " + b.length);
        while (i < a.length || j < b.length){
            System.out.println(i + " " + j);
            if (i >= a.length){
                toadd[toa] = b[j];
                toa++;
                if (j < b.length)
                    j++;
            }
            else if ( j >= b.length){
                toremove[tor] = a[i];
                tor++;
                if (i < a.length )
                    i++;
                
            }
            else if (a[i] == b[j]){
                if (i < a.length )
                    i++;
                if (j < b.length)
                    j++;
            }
            else if (a[i]<b[j]){
                //remove a[i]
                toremove[tor] = a[i];
                tor++;
                if (i < a.length )
                    i++;
                else{
                    toadd[toa] = b[j];
                    toa++;
                    if (j < b.length)
                        j++;
                }
            }
            else if (a[i] > b[j]){
                //add b[j] to the list
                toadd[toa] = b[j];
                toa++;
                if (j < b.length)
                    j++;
                else{
                    toremove[tor] = a[i];
                    tor++;
                    if (i < a.length )
                        i++;
                }
            }
            else{
                System.out.println("go on");
            }
        }
        
        for (int k=0; k < toremove.length ; k++){
            System.out.println(toremove[k]);
            System.out.println("Deleteing "+(String) map.get(Integer.toString(toremove[k])));
            //removeSlave((String) map.get(toremove[k]));
        }
        
        for (int k=0; k < toadd.length ; k++){
            System.out.println(toadd[k]);
            System.out.println("adding slave "+(String) map.get(Integer.toString(toadd[k])));
            addSlave((String) map.get(toadd[k]));
        }
    }
    
    public void removeSlave(String s1){
        try {
            connect = makeConnection();
            preparedStatement = connect
                    .prepareStatement("delete from hadoop_slaves "
                            + " where ec2ip=?");
            preparedStatement.setString(1,s1);
            preparedStatement.executeUpdate();
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
