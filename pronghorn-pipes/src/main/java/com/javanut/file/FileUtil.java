package com.javanut.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtil {

	/**
	 * Scans up file tree until the given path can be found.
	 * 
	 * @param goal suffix of the path, including the file name
	 * @return the path to desired goal or null if not found
	 */
	public static Path findPathInPath(String goal) {
		
		Path cur = Paths.get(".").toAbsolutePath();
		
		Path temp = null;
		while (!(temp = cur.resolve(goal)).toFile().exists()) {
			cur = cur.getParent();
			if (null == cur) {
				return null;
			}
		}
		return temp;
	}
	
	/**
	 * Rename backup which builds a new copy of the file in a fresh location on the disk.
	 * @param count of backups, minimum 1 maximum 9.
	 * @param file to be backed up
	 */
	public static boolean backup(int count, File file) {
		assert(count<10) :  "only supports 9 backups max";
		assert(count>0) : "must have at least 1 backup";
		if (!file.canWrite()) {
			return false; //can not back up, need rights to the file.
		}
		String root = file.getPath();
		
		File target = null;
		int i = count;
		while (--i>=0) {
			File source = i==0 ? file : new File(root+".bk"+i);
			target = new File(root+".bk"+(i+1));
			if (target.exists()) {
				target.delete();
			}
			if (source.exists()) {
				source.renameTo(target);
			}
		}
		
		//put the original file back since we renamed it.
		try {
			Files.copy(target.toPath(), file.toPath());
		} catch (IOException e) {
			target.renameTo(file);
			return false;			
		}
		return true;
	}
	
	
	
	
}
