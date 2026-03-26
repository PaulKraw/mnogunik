<?php
/**
 * Template Name: page-mnogunik
 * @package ThemeHunk
 * @subpackage Top Store
 * @since 1.0.0
 */


get_header();

$top_store_pages_sidebar = top_store_pages_sidebar(); ?>



<div id="content" class="page-content thunk-page">
        	<div class="content-wrap" >
        		<div class="container">
					    
        			<div class="main-area <?php echo esc_attr($top_store_pages_sidebar);?>">
                <?php if($top_store_pages_sidebar !=='no-sidebar' && $top_store_pages_sidebar !=='disable-left-sidebar'){get_sidebar('primary');}?>
        				<div id="primary" class="primary-content-area">
        					<div class="primary-content-wrap">
                    <div class="page-head">
                   <?php top_store_get_page_title();?>
                   <?php top_store_breadcrumb_trail();?>
                    </div>
                        <div class="thunk-content-wrap">
                        <?php
                            while( have_posts() ) : the_post();
                               get_template_part( 'template-parts/content', 'page' );
                              // If comments are open or we have at least one comment, load up the comment template.
                              if ( comments_open() || get_comments_number() ) :
                                comments_template();
                               endif;
                               endwhile; // End of the loop.
                            ?>
							q
<? 

echo __FILE__; // Полный путь до этого файла

echo dirname(__FILE__); // Путь к папке файла

echo "http://" . $_SERVER['HTTP_HOST'] . $_SERVER['REQUEST_URI'];

echo $_SERVER['DOCUMENT_ROOT'];



$root = $_SERVER['DOCUMENT_ROOT'];
$script_dir = dirname(__FILE__);
$full_url = "http://" . $_SERVER['HTTP_HOST'] . $_SERVER['REQUEST_URI'];

echo "Корень: $root\n";
echo "Текущая папка: $script_dir\n";
echo "Полный URL: $full_url\n";





$python_script = '/var/www/u1168406/data/www/paulkraw.ru/mnogunik/go.py';


$command = "python3 $python_script 2>&1";


// Выполнение и вывод результата
$output = shell_exec($command);


echo "<pre>$output</pre>";
?>
							

							

	


							
						



                         </div>



								
								
								
								
								
								
								
								
								
								
								
								
								
								
								
                      </div> <!-- end primary-content-wrap-->
        				</div> <!-- end primary primary-content-area-->
        				<?php if($top_store_pages_sidebar !=='no-sidebar' && $top_store_pages_sidebar !=='disable-right-sidebar'){ get_sidebar('secondary');}?>
                <!-- end sidebar-secondary  sidebar-content-area-->
        			</div> <!-- end main-area -->
        		</div>
        	</div> <!-- end content-wrap -->
</div> <!-- end content page-content -->
<?php get_footer();?>