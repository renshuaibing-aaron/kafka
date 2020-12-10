package kafka.utils

import scala.annotation.StaticAnnotation

/* Some helpful annotations */

/**
 * Indicates that the annotated class is meant to be threadsafe. For an abstract class it is an part of the interface that an implementation
 * must respect
 */
class threadsafe extends StaticAnnotation

/**
 * Indicates that the annotated class is not threadsafe
 */
class nonthreadsafe extends StaticAnnotation

/**
 * Indicates that the annotated class is immutable
 */
class immutable extends StaticAnnotation
