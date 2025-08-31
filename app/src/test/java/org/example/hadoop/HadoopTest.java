package org.example.hadoop;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Annotation to automatically start/stop Hadoop services for tests
 * 
 * Usage:
 * @HadoopTest
 * class MyHadoopTest {
 *   // Your tests here - Hadoop will be started before and stopped after
 * }
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(HadoopTestExtension.class)
public @interface HadoopTest {
}