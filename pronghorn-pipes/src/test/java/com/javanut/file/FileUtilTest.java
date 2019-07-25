package com.javanut.file;

import static org.junit.Assert.assertNotNull;

import java.nio.file.Path;

import org.junit.Test;

public class FileUtilTest {

	@Test
	public void testFile() {
		
		//System.out.println(Paths.get(".").toAbsolutePath());
		
		Path result = FileUtil.findPathInPath("src/main/java/com/javanut/file/FileUtil.java");
		assertNotNull(result);
		//System.out.println(result);
		
		
		//TODO: need a better way to test the backup
//		Path result2 = FileUtil.findPathInPath(".aws/credentials");
//		assertNotNull(result2);
//		//System.out.println(result2);
//		
//		FileUtil.backup(2, result2.toFile());
		
	}
	
}
