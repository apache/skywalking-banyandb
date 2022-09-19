import { createRouter, createWebHistory } from 'vue-router'
import HomeView from '../views/HomeView.vue'
import NotFoundView from '../views/NotFoundView.vue'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      redirect: '/home'
    },
    {
      path: '/home',
      name: 'Home',
      component: HomeView,
      meta: {
        keepAlive: false,
      }
    },
    {
      // will match everything
      path: '/:pathMatch(.*)',
      name: 'NotFound',
      component: NotFoundView,
      meta: {
        keepAlive: false,
      }
    },
  ]
})

export default router
