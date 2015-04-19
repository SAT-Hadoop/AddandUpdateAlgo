/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.iit.driver;

import com.mycompany.slavesupdate.Slave;
import edu.iit.masterupdate.Master;

/**
 *
 * @author supramo
 */
public class Driver {
    
    public static void main(String[] args){
        switch (Integer.parseInt(args[0])) {
            case 1:
                Master master = new Master();
                master.updateMaster();
                break;
                
            case 2:
                Slave slave = new Slave();
                slave.updateSlave();
                break;
                
            default:
                
                break;
        }
                
    }
}
